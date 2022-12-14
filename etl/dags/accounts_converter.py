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
import codecs
import io

@dag(
    schedule_interval='@daily', # TODO
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['ton'],
    user_defined_filters=dict(sanitize_run_id=lambda r: r.replace('-', '_').replace(':', '_').replace('+', '_').replace('.', '_'))
)
def accounts_increment():
    """
    Converts accounts_state DB table to parquet files in incremental fashion and uploads
    it to S3
    """

    drop_increment_table_initial = PostgresOperator(
        task_id="drop_increment_table_initial",
        postgres_conn_id="ton_db",
        sql=[
            "DROP TABLE IF EXISTS accounts_increment_{{ run_id | sanitize_run_id }}",
            "DROP TABLE IF EXISTS messages_increment_{{ run_id | sanitize_run_id }}",
            "DROP TABLE IF EXISTS transactions_increment_{{ run_id | sanitize_run_id }}"
            ]
    )

    drop_increment_table = PostgresOperator(
        task_id="drop_increment_table",
        postgres_conn_id="ton_db",
        sql=[
            "DROP TABLE accounts_increment_{{ run_id | sanitize_run_id }}",
            "DROP TABLE messages_increment_{{ run_id | sanitize_run_id }}",
            "DROP TABLE transactions_increment_{{ run_id | sanitize_run_id }}"
        ]
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
        sql=["""
        CREATE TABLE IF NOT EXISTS accounts_increment_{{ run_id  | sanitize_run_id }} (
        	state_id integer NOT NULL,
            address varchar NULL,
            check_time int8 NULL,
            last_tx_lt int8 NULL,
            last_tx_hash varchar NULL,
            balance int8 NULL,
            code_hash varchar NULL,
            "data" varchar NULL
        );""", """
        CREATE TABLE IF NOT EXISTS messages_increment_{{ run_id  | sanitize_run_id }} (
        	msg_id integer NOT NULL,
        	source varchar NULL,
            destination varchar NULL,
            value int8 NULL,
            fwd_fee int8 NULL,
            ihr_fee int8 NULL,
            created_lt int8 NULL,
            hash varchar(44) NULL,
            body_hash varchar(44) NULL,
            op int4 NULL,
            "comment" varchar NULL,
            ihr_disabled bool NULL,
            bounce bool NULL,
            bounced bool NULL,
            import_fee int8 NULL,
            out_tx_id int8 NULL,
            in_tx_id int8 NULL,
            body varchar NULL,
            utime int8 NULL
        );""","""
        CREATE TABLE IF NOT EXISTS transactions_increment_{{ run_id  | sanitize_run_id }} (
        	tx_id integer NOT NULL,
        	account varchar NULL,
        	lt int8 NULL,
            hash varchar(44) NULL,
            utime int8 NULL,
            fee int8 NULL,
            storage_fee int8 NULL,
            other_fee int8 NULL,
            transaction_type varchar NULL,
            compute_exit_code int4 NULL,
            compute_gas_used int4 NULL,
            compute_gas_limit int4 NULL,
            compute_gas_credit int4 NULL,
            compute_gas_fees int8 NULL,
            compute_vm_steps int4 NULL,
            compute_skip_reason varchar NULL,
            action_result_code int4 NULL,
            action_total_fwd_fees int8 NULL,
            action_total_action_fees int8 NULL,
            block_id int4 NULL,
            workchain int4 NOT NULL,
            shard int8 NULL,
	        seqno int4 NULL,
	        masterchain_block_id int4 NULL
        );"""]
    )

    generate_increment_accounts = PostgresOperator(
        task_id="generate_increment_accounts",
        postgres_conn_id="ton_db",
        sql=[
            """
        insert into accounts_increment_{{ run_id | sanitize_run_id }}
        select state_id, address, check_time, last_tx_lt, last_tx_hash, balance, code_hash,
        decode(replace(replace(
          case
            when length(data) % 4 != 0 then rpad(data, length(data) +  4 - length(data) % 4, '=')
            else data
          end, '_', '/'), '-', '+'), 'base64') as data
        from account_state
        where check_time >= {{ data_interval_start.int_timestamp }} and  check_time < {{ data_interval_end.int_timestamp }}
        """
        ]
    )

    generate_increment_transactions = PostgresOperator(
        task_id="generate_increment_transactions",
        postgres_conn_id="ton_db",
        sql=[
            """
        insert into transactions_increment_{{ run_id | sanitize_run_id }}
        select tx_id, account, lt, hash, utime, fee, storage_fee, other_fee, transaction_type, compute_exit_code,
         compute_gas_used, compute_gas_limit, compute_gas_credit, compute_gas_fees, compute_vm_steps, compute_skip_reason,
         action_result_code, action_total_fwd_fees, action_total_action_fees, t.block_id, workchain, shard, seqno, masterchain_block_id
        from transactions t
        join blocks b on b.block_id = t.block_id
        where utime >= {{ data_interval_start.int_timestamp }} and utime < {{ data_interval_end.int_timestamp }}
        """
        ]
    )

    generate_increment_messages_1 = PostgresOperator(
        task_id="generate_increment_messages_1",
        postgres_conn_id="ton_db",
        sql="""
        insert into messages_increment_{{ run_id | sanitize_run_id }}
        select m.msg_id, source, destination, value, fwd_fee, ihr_fee, created_lt, m.hash, body_hash, op,
            "comment", ihr_disabled, bounce, bounced, import_fee, out_tx_id, in_tx_id,
            decode(mc.body, 'base64') as body, 
            t_in.utime as utime
        from messages m
        join transactions t_in on t_in.tx_id  = m.in_tx_id 
        left join message_contents mc on mc.msg_id = m.msg_id        
        where t_in.utime >= {{ data_interval_start.int_timestamp }} and
         t_in.utime < {{ data_interval_end.int_timestamp }}
        """
    )

    generate_increment_messages_2 = PostgresOperator(
        task_id="generate_increment_messages_2",
        postgres_conn_id="ton_db",
        sql="""
        insert into messages_increment_{{ run_id | sanitize_run_id }}
        select m.msg_id, source, destination, value, fwd_fee, ihr_fee, created_lt, m.hash, body_hash, op,
            "comment", ihr_disabled, bounce, bounced, import_fee, out_tx_id, in_tx_id, 
            decode(mc.body, 'base64') as body, 
            t_out.utime as utime
        from messages m
        join transactions t_out on t_out.tx_id  = m.out_tx_id 
        left join message_contents mc on mc.msg_id = m.msg_id        
        where in_tx_id is null and t_out.utime >= {{ data_interval_start.int_timestamp }} and
         t_out.utime < {{ data_interval_end.int_timestamp }}
        """
    )

    def update_state(table_name):
        return PostgresOperator(
            task_id=f"update_state_{table_name}",
            postgres_conn_id="ton_db",

            sql="""
            insert into increment_state (table_name, start_time, end_time, processed_time, rows_count, size_bytes, file_path)
            values (
                '%s',  
                to_timestamp({{ data_interval_start.int_timestamp }}), 
                to_timestamp({{ data_interval_end.int_timestamp }}),
                now(),
                {{ ti.xcom_pull(task_ids='convert_%s', key='rows_count')}},
                {{ ti.xcom_pull(task_ids='convert_%s', key='file_size')}},
                '{{ ti.xcom_pull(task_ids='convert_%s', key='file_url')}}'            
            )
            """ % (table_name, table_name, table_name, table_name),
        )

    # based on pytonlib: https://github.com/toncenter/pytonlib/blob/main/pytonlib/utils/address.py
    def calcCRC(message):
        poly = 0x1021
        reg = 0
        message += b'\x00\x00'
        for byte in message:
            mask = 0x80
            while(mask > 0):
                reg <<= 1
                if byte & mask:
                    reg += 1
                mask >>= 1
                if reg > 0xffff:
                    reg &= 0xffff
                    reg ^= poly
        return reg.to_bytes(2, "big")

    def convert_addr(acc):
        wc, account_id = acc.split(":")
        wc, account_id = int(wc), int(account_id, 16).to_bytes(32, "big")
        tag = b'\xff' if wc == -1 else wc.to_bytes(1, "big")
        addr = b'\x11' + tag + account_id
        return codecs.decode(codecs.encode(addr+calcCRC(addr), "base64"), "utf-8").strip()

    def convert_to_parquet_and_upload(ti, table, suffix, start_time, convert_address=False, convert_to_int64=[],
                                      max_batch_size=1000000, sharding_key=None):
        postgres_hook = PostgresHook(postgres_conn_id="ton_db")
        rows_count = postgres_hook.get_first(f"select count(1) as cnt from {table}_increment_{suffix}")[0]
        logging.info(f"Total rows: {rows_count}")
        bucket = Variable.get('etl.ton.s3.bucket')
        total_file_size = 0
        batches_count = 1
        file_path = None
        start_time = pendulum.parse(start_time)
        s3 = S3Hook('s3_conn')

        if rows_count > max_batch_size and sharding_key is not None:
            batches_count += rows_count // max_batch_size
        for batch_idx in range(batches_count):
            if batches_count == 1:
                query = f"select * from {table}_increment_{suffix}"
            else:
                query = f"select * from {table}_increment_{suffix} where {sharding_key} % {batches_count} = {batch_idx}"
            df = postgres_hook.get_pandas_df(query)
            for field in convert_to_int64:
                df[field] = df[field].astype('Int64')
            logging.info(f"[{batch_idx}] Get results for {table} with shape {df.shape}")
            if df.shape[0] == 0:
                ti.xcom_push(key='rows_count', value=df.shape[0])
                ti.xcom_push(key='file_size', value=0)
                ti.xcom_push(key='file_url', value='')
            else:
                if convert_address:
                    df['address'] = df.account.map(convert_addr)
                buffers = []
                buff = io.BytesIO()
                df.to_parquet(buff)
                file_size = buff.getbuffer().nbytes
                MAX_SIZE = 50000000
                if file_size > MAX_SIZE:
                    del buff
                    logging.info(f"Parquet file is too large: {file_size}")
                    num_parts = file_size // 50000000 + 1
                    while True:
                        buffers = []
                        chunk_size = df.shape[0] // num_parts + 1
                        logging.info(f"Splitting df into {num_parts} parts {chunk_size} lines each")
                        file_size = 0
                        finished = True
                        for i in range(num_parts):
                            chunk = df[i * chunk_size:(i+1) * chunk_size]
                            buff = io.BytesIO()
                            chunk.to_parquet(buff)
                            buffers.append(buff)
                            chunk_file_size = buff.getbuffer().nbytes
                            logging.info(f"New chunk #{i}: {chunk_file_size}")
                            if chunk_file_size > MAX_SIZE:
                                num_parts += 1
                                logging.info(f"Chunk over max size, increasing number of chunks: {num_parts}")
                                del buff
                                del chunk
                                buffers = []
                                finished = False
                                break
                            file_size += chunk_file_size
                        if finished:
                            break
                else:
                    file_size = buff.getbuffer().nbytes
                    buffers.append(buff)
                total_file_size += file_size

                logging.info(f"Dataframe converted to parquet, size: {file_size}, {len(buffers)} chunks")

                for idx, buff in enumerate(buffers):
                    buff.seek(0)
                    file_suffix = "" if batches_count == 1 else f"batch_{batch_idx}"
                    file_suffix += "" if len(buffers) == 1 else f"_{idx}"
                    file_path = f"dwh/staging/{table}/date={start_time.strftime('%Y%m')}/{start_time.strftime('%Y%m%d')}{file_suffix}.parquet"
                    s3.load_file_obj(buff, key=file_path, bucket_name=bucket, replace=True)
            ti.xcom_push(key='rows_count', value=rows_count)
            ti.xcom_push(key='file_size', value=total_file_size)
            ti.xcom_push(key='file_url', value=f"s3://{bucket}/{file_path}" if file_path is not None else None) # the last one

    def convert_to_parquet_and_upload_task(table, convert_address=False, convert_to_int64=[], sharding_key=None):
        return PythonOperator(
            task_id=f'convert_{table}',
            python_callable=convert_to_parquet_and_upload,
            op_kwargs={
                'sharding_key': sharding_key,
                'convert_address': convert_address,
                'table': table,
                'convert_to_int64': convert_to_int64,
                'suffix': '{{ run_id | sanitize_run_id }}',
                'start_time': '{{ data_interval_start }}'
            }
        )

    create_state_table >> drop_increment_table_initial >> create_increment_table >> generate_increment_accounts
    generate_increment_accounts >> generate_increment_transactions >> generate_increment_messages_1 >> generate_increment_messages_2
    convert_accounts = convert_to_parquet_and_upload_task("accounts")
    convert_transactions = convert_to_parquet_and_upload_task("transactions", convert_address=True, sharding_key='tx_id',
                                                              convert_to_int64=['compute_exit_code', 'compute_gas_used', 'compute_gas_limit',
                                                                                'compute_gas_credit', 'compute_gas_fees', 'compute_vm_steps',
                                                                                'action_result_code', 'action_total_fwd_fees', 'action_total_action_fees',
                                                                                'masterchain_block_id'])
    convert_messages = convert_to_parquet_and_upload_task("messages", sharding_key='msg_id',
                                                          convert_to_int64=['op', 'import_fee', 'out_tx_id', 'in_tx_id'])
    generate_increment_messages_2 >>\
        convert_accounts >> update_state ("accounts") >> \
        convert_transactions >> update_state("transactions")  >> \
        convert_messages >>update_state("messages") >> drop_increment_table


accounts_increment_dag = accounts_increment()

    