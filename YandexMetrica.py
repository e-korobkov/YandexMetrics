import time
import datetime
import pathlib
import requests
import json
import pandas as pd
import os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException, AirflowWebServerTimeout, AirflowBadRequest, \
    AirflowNotFoundException, AirflowException, AirflowFailException
from airflow.sensors.python import PythonSensor


start_date = pendulum.datetime(2022, 1, 10)  # launch of a promotion
status_tr = {
    'processed': 'обработан (готов к выгрузке)',
    'canceled': 'отменён',
    'processing_failed': 'ошибка при обработке',
    'created': 'создан (не готов к выгрузке)',
    'cleaned_by_user': 'очищен пользователем',
    'cleaned_automatically_as_too_old': 'очищен автоматически'}


def get_conf():
    token = Variable.get("token_pass")
    time_var = dict(Variable.get('yandex_metrica', deserialize_json=True))
    counter_id = time_var['counter_id']
    bd_tables = time_var['reports']
    return token, counter_id, bd_tables


def connect_db():
    num_attempts = 5
    while True:
        try:
            print('Trying to connect to database')
            pg_hook = PostgresHook(postgres_conn_id="Postgres_metrica")
            connection = pg_hook.get_conn()
            cursor = connection.cursor()
        except Exception as err:
            if num_attempts != 0:
                time.sleep(10)
                num_attempts -= 1
            else:
                raise AirflowSkipException("Can't connect to database")
        else:
            return connection, cursor


def error(r):
    get_logs = json.loads(r.text)
    error_type = get_logs['errors'][0].get('error_type')
    message = get_logs['errors'][0].get('message')
    if r.status_code == 429:
        # Number of simultaneous requests exceeded
        print(f'Error № {error_type}, reason: {message}')
        print('Sleeping until restrictions are lifted')
        # Sleep
        time.sleep((
            pendulum.tomorrow('Europe/Moscow') -
            pendulum.now('Europe/Moscow')
            ).seconds + 600)
        return
    print('r', r.status_code)
    print('get_logs', get_logs)
    print('error_type', error_type)
    print('message', message)
    raise AirflowFailException(f'Error № {error_type}, reason: {message}')


def _check_date(logical_date, data_interval_start, **kwargs):
    source = kwargs["source"]
    # Check for manual start
    if pendulum.today().date() == logical_date.date():
        print('first')
        date1 = logical_date.add(days=-1).to_date_string()
        date2 = logical_date.add(days=-1).to_date_string()
    else:
        print('second')
        date1 = data_interval_start.to_date_string()
        date2 = data_interval_start.to_date_string()

    kwargs['task_instance'].xcom_push(key=f'{source}_order_date', value={'date1': date1, 'date2': date2})


def _create_table(**kwargs):
    token, counter_id, bd_tables = get_conf()
    table_name = kwargs['table_name']
    connection, cursor = connect_db()

#    for key in bd_tables.keys():
        #table_name = bd_tables.get(key).get('source')
    col_name = bd_tables.get(table_name).get('name').split(',')
    col_type = bd_tables.get(table_name).get('type').split(', ')

    time_list = []
    for num in range(len(col_name)):
        time_list.append(f'"{col_name[num].lower()}" {col_type[num]}')
    text = ','.join(time_list)
    print(text)
    # Checking for the first run
    if start_date.date() == kwargs['data_interval_start'].date():
        """ Create base table """
        text = ', '.join(time_list)
        request = f"CREATE TABLE IF NOT EXISTS {table_name} ({text})"
        cursor.execute(request)
        connection.commit()
        request = f"CREATE TABLE IF NOT EXISTS date_{table_name} (date date)"
        cursor.execute(request)
        connection.commit()
        """ End create table """

    cursor.close()
    connection.close()


def _python_sensor(**kwargs):
    print('start_date:', start_date.date())
    print('kwargs["data_interval_start"]:', kwargs['data_interval_start'].date())
    table_name = kwargs['table_name']
    connection, cursor = connect_db()
    if start_date.date() == kwargs['data_interval_start'].date():  # first run
        print('First start')
        return True
    else:
        prev_day = kwargs['data_interval_start'].add(days=-1).date().to_date_string()
        sql = f"select max(date) from public.date_{table_name} where date = '{prev_day}'"
        print(sql)
        cursor.execute(sql)
        rez = cursor.fetchall()
        cursor.close()
        connection.close()
        if len(rez) > 0 and isinstance(rez[0][0], datetime.date):
            print('non empty')
            return True
        else:
            print('Empty')
            return False


def _server_reports(**kwargs):
    token, counter_id, bd_tables = get_conf()
    source = kwargs['source']
    date_xcom = kwargs['ti'].xcom_pull(task_ids=f'{source}_check_date', key=f"{source}_order_date")
    date1 = date_xcom['date1']
    date2 = date_xcom['date2']
    url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests'
    cycle = 5
    while True:
        try:
            r = requests.get(url, headers={"Authorization": f'OAuth {token}'})
        except AirflowWebServerTimeout as err:
            raise AirflowSkipException('TimeOut')
        else:
            if r.status_code == 200:
                get_logs = json.loads(r.text)
                requests_server = get_logs.get('requests')
                check_dict = dict(
                    date1=date1,
                    date2=date2,
                    source=kwargs['source'])
                check_col = set(bd_tables.get(kwargs['source'])['name'].split(','))
                # Setting check
                for part in requests_server:
                    for my_key in check_dict.keys():
                        if my_key in part.keys():
                            if part.get(my_key) != check_dict.get(my_key):
                                break  # Different parameters
                        else:
                            break  # Different parameters
                    else:
                        fields = set(part.get('fields'))
                        if len(check_col.difference(fields)) == 0:
                            kwargs['task_instance'].xcom_push(key=f'{kwargs["source"]}_reports',
                                                              value={'log_request': part})
                            # If the report is ready, go to download
                            # If the report is not ready, go to waiting
                            print('The Yandex server already has a report with our parameters')
                            return None
                print('Request to create a new report')
                kwargs['task_instance'].xcom_push(key=f'{kwargs["source"]}_reports', value={'log_request': None})
                return None
            elif r.status_code in [400, 403, 429]:
                error(r)
            else:
                cycle -= 1
                time.sleep(30)
                if cycle <= 0:
                    raise AirflowFailException('No response from Yandex server')


def _branching(**kwargs):
    source=kwargs['source']
    table_name = kwargs['table_name']
    report = kwargs['ti'].xcom_pull(task_ids=f'{source}_server_reports', key=f'{source}_reports')
    if report.get('log_request') is None:
        return f"{table_name}_possibility"

    elif report['log_request'].get('status') == 'created':
        return f"{table_name}_first_join_branch"
    else:
        # 'status' == 'processed'
        return f"{table_name}_full"


def _possibility(**kwargs):
    token, counter_id, bd_tables = get_conf()
    source = kwargs['source']
    date_xcom = kwargs['ti'].xcom_pull(task_ids=f'{source}_check_date', key=f"{source}_order_date")
    date1 = date_xcom['date1']
    date2 = date_xcom['date2']
    fields = bd_tables.get(source).get('name')
    url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests/evaluate'\
        f'?date1={date1}&date2={date2}&fields={fields}&source={source}'
    cycle = 15
    while True:
        try:
            r = requests.get(url, headers={"Authorization": f'OAuth {token}'})
        except AirflowWebServerTimeout as err:
            raise AirflowFailException('TimeOut')
        except AirflowException as err:
            raise AirflowException(f'Cannot execute {err}')
        else:
            if r.status_code == 200:
                get_logs = json.loads(r.text)
                if get_logs.get('log_request_evaluation')['possible'] is True:
                    return get_logs
                else:
                    cycle -= 1
                    time.sleep(30)
                    if cycle <= 0:
                        raise AirflowFailException(f'No way to generate a report')
            elif r.status_code in [400, 403, 429]:
                error(r)
            else:
                cycle -= 1
                time.sleep(30)
                if cycle <= 0:
                    raise AirflowFailException('No response from the server '
                                               '"About the possibility to generate a report"')

# Creates a report on the server
def _logrequests(**kwargs):
    token, counter_id, bd_tables = get_conf()
    source = kwargs['source']
    date_xcom = kwargs['ti'].xcom_pull(task_ids=f'{source}_check_date', key=f"{source}_order_date")
    date1 = date_xcom['date1']
    date2 = date_xcom['date2']
    fields = bd_tables.get(source).get('name')
    cycle = 10
    url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests'\
        f'?date1={date1}&date2={date2}&fields={fields}&source={source}'
    print(url)
    try:
        r = requests.post(url, headers={"Authorization": f'OAuth {token}'})
    except AirflowWebServerTimeout as err:
        time.sleep(60)
        print('AirflowWebServerTimeout')
        cycle -= 1
        if cycle <= 0:
            raise AirflowFailException('TimeOut')
    else:
        get_logs = json.loads(r.text)

        if r.status_code == 200:
            print('200')
            rep_in_progress = get_logs.get('log_request')
            status = rep_in_progress.get('status')
            print(status)
            if status in ['processed', 'created'] :
                kwargs['task_instance'].xcom_push(key=f'{source}_reports', value={
                    'log_request': rep_in_progress})
            else:
                time.sleep(30)
                cycle -= 1
                if cycle <= 0:
                    raise AirflowBadRequest(f'{status_tr.get(status)}')
        elif r.status_code in [400, 403, 429]:
            print(r.status)
            error(r)
        else:
            st1 = f"The required parameter is not specified or the filter complexity exceeds the set limits."
            st2 = f"Error description: {get_logs.get('errors')[0]['error_type']}"
            st3 = f"The reason for the error: {get_logs.get('errors')[0]['message']}"
            print(f'{st1}\n{st2}\n{st3}')
            raise AirflowFailException(f'The required parameter is not specified or the filter complexity exceeds '
                                       f'the set limits')


# Checks that the report is ready
def _readiness_check(**kwargs):
    token, counter_id, bd_tables = get_conf()
    source = kwargs["source"]
    date_xcom = kwargs['ti'].xcom_pull(task_ids=f'{source}_logrequests', key=f"{source}_reports")
    if date_xcom is None:
        date_xcom = kwargs['ti'].xcom_pull(task_ids=f'{source}_server_reports', key=f"{source}_reports")
    request_id = date_xcom.get('log_request').get('request_id')
    time.sleep(30)
    cycle = 20
    url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}'
    while True:
        try:
            r = requests.get(url, headers={"Authorization": f'OAuth {token}'})
        except AirflowWebServerTimeout as err:
            raise AirflowFailException('TimeOut')
        else:
            if r.status_code == 200:
                get_logs = json.loads(r.text)
                if not get_logs:
                    raise AirflowSkipException(f'No report on the server')
                status = get_logs['log_request'].get('status')
                print(f'Waiting for the report to be ready, cycle {cycle-20} status {status}')
                if status == 'processed':
                    # Check for data availability
                    print('Report size:', get_logs['log_request'].get('size'))
                    kwargs['task_instance'].xcom_push(key=f'{source}_reports',
                                                      value={'log_request': get_logs['log_request']})
                    break
                elif status == 'created':
                    time.sleep(60)
                    cycle -= 1
                    if cycle <= 0:
                        raise AirflowFailException(f'No response from Yandex in the allotted time')
                elif status == 'cleaned_by_user':
                    raise AirflowSkipException("The report was cleared by the user earlier!")
                elif status == 'processing_failed':
                    raise AirflowSkipException("Report generation error")
                else:
                    raise AirflowFailException(f'Report creation error: {status_tr.get(status)}')
            elif r.status_code in [400, 403, 429]:
                error(r)
            else:
                print(f'Waiting for the report to be ready returned: {r.status_code}')
                time.sleep(30)
                cycle -= 1
                if cycle <= 0:
                    raise AirflowFailException('The server did not respond to the report request')


def _branching_second(**kwargs):
    source = kwargs['source']
    table_name = kwargs['table_name']
    report = kwargs['ti'].xcom_pull(task_ids=f'{source}_readiness_check', key=f'{source}_reports')
    if int(report['log_request'].get('size')) == 0:
        print(report['log_request'].get('parts'))
        print(type(report['log_request'].get('parts')))
        for part in report['log_request'].get('parts'):
            if part.get('size') != 0:
                return f"{table_name}_full"   # 'status' == 'processed' and size > 0
        else:
            return f"{table_name}_add_report_to_bd"  # 'status' == 'processed' and size = 0
    else:
        return f"{table_name}_full"   # 'status' == 'processed' and size > 0


def _download_data(**kwargs):
    token, counter_id, bd_tables = get_conf()
    source = kwargs["source"]
    """ Check where the dag came from """
    date_xcom = kwargs['ti'].xcom_pull(task_ids=f'{source}_readiness_check', key=f"{source}_reports")
    if date_xcom is None:
        # There is a report ready on the server
        date_xcom = kwargs['ti'].xcom_pull(task_ids=f'{source}_server_reports', key=f"{source}_reports")
    """ End of check where dag came from """
    # A plug in case of a Yandex failure
    if date_xcom.get('log_request') is None:
        raise AirflowFailException(f'The download_data function did not pass the Report data')

    request_id = date_xcom['log_request'].get('request_id')
    """ Let's get rid of the "special features" """
    fields = ','.join(date_xcom['log_request'].get('fields')).split(',')
    """ End Let's get rid of the "special features" """
    print(fields)
    time_df = pd.DataFrame(data=None, columns=fields)

    for part_dict in date_xcom['log_request'].get('parts'):
        print(f'Start downloading {part_dict.get("part_number")}')
        url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}' \
            f'/logrequest/{request_id}/part/{part_dict.get("part_number")}/download'
        try:
            r = requests.get(url, headers={"Authorization": f'OAuth {token}'})
        except AirflowWebServerTimeout as err:
            raise AirflowFailException(f'TimeOut for part {part_dict.get("part_number")}')
        else:
            if r.status_code == 200:
                data = [x.split('\t') for x in r.content.decode('utf-8').split('\n')[:-1]]
                df_ym = pd.DataFrame(data[1:], columns=fields)
                # Let's convert text to lowercase
                for col in df_ym.columns:
                    df_ym[col] = df_ym[col].map(lambda x: x.lower() if isinstance(x, str) else x)
                time_df = pd.concat([df_ym, time_df])
            elif r.status_code in [400, 403, 429]:
                error(r)
    else:
        print('Старт записи DF')
        p = '/opt/airflow/local_volume/yandex_metrica'
        pathlib.Path(p).mkdir(parents=True, exist_ok=True)

        time_df = time_df.rename(columns={'from': 'from1','From': 'from1'},inplace=False)
        time_df.to_pickle(f'{p}/{request_id}.pkl')

        kwargs['task_instance'].xcom_push(key=f'{source}_reports', value={
            'pickle_df': request_id,
            'date1': date_xcom['log_request'].get('date1'),
            'date2': date_xcom['log_request'].get('date2')})
        print('The download_data function is finished.')
        return None


def _write_data_bd(**kwargs):
    table_name = kwargs['table_name']
    source = kwargs['source']

    date_xcom = kwargs['ti'].xcom_pull(task_ids=f'{source}_download_data', key=f"{source}_reports")
    if date_xcom.get('pickle_df') is None:
        raise AirflowFailException(f'Функции write_data_bd не переданы данные об DataFrame')

    df_name = date_xcom['pickle_df']
    p = f'/opt/airflow/local_volume/yandex_metrica/{df_name}.pkl'
    unpicled_df = pd.read_pickle(p)
    connection, cursor = connect_db()
    try:
        """ Delete data from Postgres """
        if source == 'hits':
            date_col = 'ym:pv:date'
        else:
            date_col = 'ym:s:date'
        sql_query = f"DELETE FROM public.{table_name} WHERE " \
                    f'"{date_col}"' + f" >= '{date_xcom.get('date1')}' AND " \
                    f'"{date_col}"' + f" <= '{date_xcom.get('date2')}'"

        cursor.execute(sql_query)
        """ END delete data from Postgres """
        """ Into data to the Postgres """
        tuples = [tuple(x) for x in unpicled_df.to_numpy()]
        time_list = []
        for num in unpicled_df:
            time_list.append(f'"{num.lower()}"')
        cols = ','.join(time_list)
        query = f'INSERT INTO "{table_name}"({cols}) VALUES({"%s," * (unpicled_df.values.shape[1] - 1)}{"%s" * 1})'
        cursor.executemany(query, tuples)
        """ END into data to the Postgres """
        cursor.close()

        connection.commit()
        connection.close()
        kwargs['task_instance'].xcom_push(key=f'{source}_reports', value=df_name)
    except Exception as err:
        print(err)
        raise AirflowFailException("Can't write to database")


def _add_report_to_bd(data_interval_start, **kwargs):
    table_name = kwargs['table_name']
    connection, cursor = connect_db()
    try:
        """ Delete data from Postgres """
        sql_query = f"DELETE FROM public.date_{table_name} WHERE " \
                    f"date = '{data_interval_start.to_date_string()}'"
        cursor.execute(sql_query)
        """ END delete data from Postgres """
        """ Into data to the Postgres """
        sql_query = f"INSERT INTO public.date_{table_name}(date) VALUES('{data_interval_start.to_date_string()}')"
        cursor.execute(sql_query)
        """ END into data to the Postgres """
        cursor.close()
        connection.commit()
        connection.close()
    except Exception as err:
        print(err)
        raise AirflowFailException("Can't write to database")


def _delete_report(**kwargs):
    source = kwargs['source']
    df_name = kwargs['ti'].xcom_pull(task_ids=f'{source}_write_data_bd', key=f"{source}_reports")
    f = f'/opt/airflow/local_volume/yandex_metrica/{df_name}.pkl'
    try:
        os.remove(f)
    except Exception as err:
        print(err)
        raise AirflowFailException(f"Can't delete pickle data {df_name}")


def _delete_from_yandex(**kwargs):
    token, counter_id, bd_tables = get_conf()
    source = kwargs['source']
    request_id = kwargs['ti'].xcom_pull(task_ids=f'{source}_write_data_bd', key=f"{source}_reports")
    cycle = 20
    url = f'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}/clean?'

    while True:
        try:
            r = requests.post(url, headers={"Authorization": f'OAuth {token}'})
        except AirflowFailException as err:
            raise AirflowFailException('TimeOut delete_from_yandex')
        else:
            if r.status_code == 200:
                get_logs = json.loads(r.text)
                if not get_logs:
                    raise AirflowFailException(f'No report on the server')
                status = get_logs['log_request'].get('status')
                print(f'Waiting to delete a report {request_id}, cycle {cycle-20} status {status}')
                if status == 'cleaned_by_user':
                    print('Deletion performed')
                    break
                else:
                    print('Unknown status')
                    time.sleep(10)
                    cycle -= 1
                    if cycle <= 0:
                        raise AirflowFailException(f"Can't delete report {request_id} on the server")
            elif r.status_code in [400, 403, 429]:
                error(r)
            else:
                print(f'Waiting to delete a report {request_id}, cycle {cycle-20} status {r.status_code}')
                time.sleep(10)
                cycle -= 1
                if cycle <= 0:
                    raise AirflowFailException(f"Can't delete report {request_id} on the server")


def create_pipeline(suffix, dag_):
    token, counter_id, bd_tables = get_conf()
    table_name = suffix
    source = bd_tables[table_name].get('source')

    check_date = PythonOperator(
        task_id=f'{table_name}_check_date',
        python_callable=_check_date,
        op_kwargs={'table_name': table_name, 'source': source},
        dag=dag_)

    create_table = PythonOperator(
        task_id=f'{table_name}_create_table',
        python_callable=_create_table,
        op_kwargs={'table_name': table_name, 'source': source},
        dag=dag_)

    python_sensor = PythonSensor(
        task_id=f'{table_name}_python_sensor',
        python_callable=_python_sensor,
        op_kwargs={'table_name': table_name, 'source': source},
        mode='reschedule',
        dag=dag_)

    server_reports = PythonOperator(
        task_id=f'{table_name}_server_reports',
        python_callable=_server_reports,
        op_kwargs={'source': source},
        dag=dag_)

    branching = BranchPythonOperator(
        task_id=f'{table_name}_branching',
        python_callable=_branching,
        op_kwargs={'table_name': table_name, 'source': source},
        dag=dag_)

    possibility = PythonOperator(
        task_id=f'{table_name}_possibility',
        python_callable=_possibility,
        op_kwargs={'source': source},
        dag=dag_)

    logrequests = PythonOperator(
        task_id=f'{table_name}_logrequests',
        python_callable=_logrequests,
        op_kwargs={'source': source},
        dag=dag_)

    readiness_check = PythonOperator(
        task_id=f'{table_name}_readiness_check',
        python_callable=_readiness_check,
        op_kwargs={'source': source},
        dag=dag_)

    first_join_branch = DummyOperator(
        task_id=f'{table_name}_first_join_branch',
        trigger_rule='none_failed',
        dag=dag_)

    second_branching = BranchPythonOperator(
        task_id=f'{table_name}_empty',
        python_callable=_branching_second,
        op_kwargs={'table_name': table_name, 'source': source},
        dag=dag_)

    second_join_branch = DummyOperator(
        task_id=f'{table_name}_full',
        trigger_rule='none_failed',
        dag=dag_)

    download_data = PythonOperator(
        task_id=f'{table_name}_download_data',
        python_callable=_download_data,
        op_kwargs={'source': source},
        dag=dag_)

    write_data_bd = PythonOperator(
        task_id=f'{table_name}_write_data_bd',
        python_callable=_write_data_bd,
        op_kwargs={'table_name': table_name, 'source': source},
        dag=dag_)

    delete_pickle = PythonOperator(
        task_id=f'{table_name}_delete_report',
        python_callable=_delete_report,
        op_kwargs={'source': source},
        dag=dag_)

    delete_from_yandex = PythonOperator(
        task_id=f'{table_name}_delete_from_yandex',
        python_callable=_delete_from_yandex,
        op_kwargs={'source': source},
        dag=dag_)

    add_report_to_bd = PythonOperator(
        task_id=f'{table_name}_add_report_to_bd',
        python_callable=_add_report_to_bd,
        op_kwargs={'table_name': table_name, 'source': source},
        trigger_rule="none_failed",
        dag=dag_)

    check_date >> create_table >> python_sensor >> server_reports >> branching
    branching >> [possibility, first_join_branch, second_join_branch]
    possibility >> logrequests >> first_join_branch >> readiness_check >> second_branching
    second_branching >> [second_join_branch, add_report_to_bd]
    second_join_branch >> download_data >> write_data_bd >> [delete_pickle, delete_from_yandex, add_report_to_bd]


with DAG(
    dag_id='YandexMetrica',
    schedule_interval='@daily',
    start_date=start_date,
    template_searchpath='opt/airflow/local_volume/',
    render_template_as_native_obj=True
) as dag:
    for elem in ["visits", "hits"]:
        create_pipeline(elem, dag)
