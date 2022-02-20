import pandas as pd
from helper_functions import get_engine_tcpip
import timeit
start = timeit.default_timer()

dest_config = {
    "host": "localhost",
    "port": 3306,
    "database": "db_account_management",
    "user": "root",
    "password": "tnbui115"}

qa_config = {
    "host": "52.16.7.222",
    "port": 3306,
    "database": "pheno20",
    "user": "phenome",
    "password": "fCK@MiV^84wP"}

dest_engine = get_engine_tcpip(dest_config)
src_engine = get_engine_tcpip(qa_config)

conn_id = 1
max_ual = int(pd.read_sql_query("select max_ual from max_ual where installation_id = {}".format(conn_id), dest_engine)
              .values[0])
# print(max_ual)
sql = "SELECT id, user_id, date, action_id FROM pheno20.users_actions_log where id > {} limit 100".format(max_ual)
chunk_size = 100000
i = 0

with src_engine.begin() as src_engine:
    with dest_engine.begin() as dest_engine:
        for chunk in pd.read_sql_query(sql, src_engine, chunksize=chunk_size):
            chunk['installation_id'] = conn_id
            i += chunk.shape[0]
            chunk.to_sql('am_ual', dest_engine, if_exists='append', schema='db_account_management', index=False)
            print('{} rows copied'.format(i))
    # print(i)
    # print(df.head(5))
# print(df.columns)

end = timeit.default_timer()
print("Process time: {}".format(end - start))
