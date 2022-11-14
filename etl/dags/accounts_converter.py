from airflow.decorators import dag, task
from airflow.models import Variable
from airflow import AirflowException
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook

from datetime import datetime, timedelta
import logging
import pendulum
import io

@dag(
    schedule_interval='@daily', # TODO
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['ton'],
    user_defined_filters=dict(sanitize_run_id=lambda r: r.replace('-', '_').replace(':', '_').replace('+', '_'))
)
def accounts_increment():
    """
    Converts accounts_state DB table to parquet files in incremental fashion and uploads
    it to S3
    """

    drop_increment_table_initial = PostgresOperator(
        task_id="drop_increment_table_initial",
        postgres_conn_id="ton_db",
        sql="DROP TABLE IF EXISTS accounts_increment_{{ run_id | sanitize_run_id }}",
    )

    drop_increment_table = PostgresOperator(
        task_id="drop_increment_table",
        postgres_conn_id="ton_db",
        sql="DROP TABLE accounts_increment_{{ run_id | sanitize_run_id }}",
    )

    create_state_table = PostgresOperator(
        task_id="create_state_table",
        postgres_conn_id="ton_db",
        sql="""
        CREATE TABLE IF NOT EXISTS increment_state (
        	id bigserial NOT NULL,
            table_name varchar,
            start_time timestamp with time zone NOT NULL,
            end_time timestamp with time zone NOT NULL,
            processed_time timestamp with time zone NOT NULL,
            rows_count int8 NOT NULL,
            size_bytes int8 NOT NULL,
            file_path varchar NOT NULL
        );""",
    )

    create_increment_table = PostgresOperator(
        task_id="create_increment_table",
        postgres_conn_id="ton_db",
        sql="""
        CREATE TABLE IF NOT EXISTS accounts_increment_{{ run_id  | sanitize_run_id }} (
        	state_id integer NOT NULL,
            address varchar NULL,
            check_time int8 NULL,
            last_tx_lt int8 NULL,
            last_tx_hash varchar NULL,
            balance int8 NULL,
            code_hash varchar NULL,
            "data" varchar NULL
        );""",
    )

    generate_increment = PostgresOperator(
        task_id="generate_increment",
        postgres_conn_id="ton_db",
        sql="""
        insert into accounts_increment_{{ run_id | sanitize_run_id }}
        select state_id, address, check_time, last_tx_lt, last_tx_hash, balance, code_hash,
        decode(replace(replace(data, '_', '/'), '-', '+'), 'base64') as data
        from account_state
        where check_time >= {{ data_interval_start.int_timestamp }} and  check_time < {{ data_interval_end.int_timestamp }}
        """,
    )

    update_state = PostgresOperator(
        task_id="update_state",
        postgres_conn_id="ton_db",

        sql="""
        insert into increment_state (table_name, start_time, end_time, processed_time, rows_count, size_bytes, file_path)
        values (
            'accounts',  
            to_timestamp({{ data_interval_start.int_timestamp }}), 
            to_timestamp({{ data_interval_end.int_timestamp }}),
            now(),
            {{ ti.xcom_pull(task_ids='convert_to_parquet_and_upload', key='rows_count')}},
            {{ ti.xcom_pull(task_ids='convert_to_parquet_and_upload', key='file_size')}},
            '{{ ti.xcom_pull(task_ids='convert_to_parquet_and_upload', key='file_url')}}'            
        )
         
        """,
    )

    def convert_to_parquet_and_upload(ti, suffix, start_time):
        query = f"select * from accounts_increment_{suffix}"
        postgres_hook = PostgresHook(postgres_conn_id="ton_db")
        df = postgres_hook.get_pandas_df(query)
        logging.info(f"Get results with shape {df.shape}")
        buff = io.BytesIO()
        df.to_parquet(buff)
        file_size = buff.getbuffer().nbytes
        logging.info(f"Dataframe converted to parquet, size: {file_size}")
        buff.seek(0)
        s3 = S3Hook('s3_conn')

        start_time = pendulum.parse(start_time)
        file_path = f"dwh/staging/accounts/date={start_time.strftime('%Y%m')}/{start_time.strftime('%Y%m%d')}.parquet"
        bucket = Variable.get('etl.ton.s3.bucket')
        s3.load_file_obj(buff, key=file_path, bucket_name=bucket, replace=True)
        ti.xcom_push(key='rows_count', value=df.shape[0])
        ti.xcom_push(key='file_size', value=file_size)
        ti.xcom_push(key='file_url', value=f"s3://{bucket}/{file_path}")

    convert_to_parquet_and_upload_task = PythonOperator(
        task_id='convert_to_parquet_and_upload',
        python_callable=convert_to_parquet_and_upload,
        op_kwargs={
            'suffix': '{{ run_id | sanitize_run_id }}',
            'start_time': '{{ data_interval_start }}'
        }
    )

    create_state_table >> drop_increment_table_initial >> create_increment_table >> generate_increment
    generate_increment >> convert_to_parquet_and_upload_task >> [update_state, drop_increment_table]


accounts_increment_dag = accounts_increment()

    