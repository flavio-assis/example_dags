import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from dateutil.parser import parse

LOGGER = logging.getLogger(__name__)
BASE_LOG_FOLDER = conf['core']['BASE_LOG_FOLDER']
cleanup_config = Variable.get('cleanup_config', deserialize_json=True, default_var='path')


def is_empty_directory(path):
    return len(os.listdir(path)) == 0


def is_dated_file(file_path, max_age_in_days, ds, remove_scheduler_logs):
    if not remove_scheduler_logs:
        if 'scheduler' in file_path:
            return False, 0
    age = os.path.getmtime(file_path)
    return age < (parse(ds) - timedelta(max_age_in_days)).timestamp(), age


def delete_dated_files(directory, max_age_in_days, remove_scheduler_logs, ds, **kwargs):
    LOGGER.info(f'Starting from dir: {directory}\n')
    LOGGER.info(f'Searching all dated files {max_age_in_days} days from {ds}...')
    deleted_files = []
    for dir_path, dir_name, file in os.walk(directory, topdown=False):
        for file_name in file:
            full_file_path = os.path.join(dir_path, file_name)
            is_valid, age = is_dated_file(
                full_file_path,
                max_age_in_days=max_age_in_days,
                ds=ds,
                remove_scheduler_logs=remove_scheduler_logs)
            if is_valid:
                LOGGER.info(f'Dated file found: {full_file_path}. '
                            f'Age: {datetime.fromtimestamp(age).isoformat()}. '
                            'It will be deleted')
                deleted_files.append(full_file_path)
                os.remove(full_file_path)

    LOGGER.info(f'Execution finished. Found {len(deleted_files)} dated file(s)')
    return deleted_files


def delete_empty_folders(directory):
    LOGGER.info(f'Starting from dir: {directory}\n')
    LOGGER.info(f'Searching all empty folders...')

    empty_directories = []
    for dir_path, dir_name, file in os.walk(directory, topdown=False):
        if is_empty_directory(dir_path):
            LOGGER.info(f'Empty folder found: {dir_path}. Removing it')
            os.rmdir(dir_path)
            empty_directories.append(dir_path)
    LOGGER.info(f'Execution finished. Found {len(empty_directories)} empty folder(s)')
    return empty_directories


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(year=2020, month=1, day=1, hour=0, minute=0),
    'retry_delay': timedelta(minutes=1),
    'delay_on_limit_secs': 5,
}

dag = DAG(
    dag_id='airflow_logs_cleanup',
    schedule_interval='@daily',
    default_args=default_args
)

delete_dated_files_task = PythonOperator(
    task_id='delete_dated_files',
    provide_context=True,
    python_callable=delete_dated_files,
    op_kwargs={
        'directory': BASE_LOG_FOLDER,
        'max_age_in_days': int(cleanup_config.get('max_age_in_days', 30)),
        'remove_scheduler_logs': bool(cleanup_config.get('remove_scheduler_logs', False))
    },
    dag=dag
)

delete_empty_folders_task = PythonOperator(
    task_id='delete_empty_folders',
    provide_context=False,
    python_callable=delete_empty_folders,
    op_kwargs={
        'directory': BASE_LOG_FOLDER,
    },
    dag=dag
)

delete_dated_files_task >> delete_empty_folders_task
