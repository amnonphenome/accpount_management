import sqlalchemy
import pandas as pd


def update_fixed_tables():
    # local db_account_management engine
    engine_local = sqlalchemy.create_engine("mysql+pymysql://root:tnbui115@localhost:3306/db_account_management")

    # qa server engine
    engine_qa = get_engine_qa()

    # update fixed tables in db_account_management
    # fixed_db_tables = ["users_actions", "users_actions_categories", "application_modules", "users_actions_types"]
    fixed_db_tables = ["users_actions_categories"]



# read>update for all fixed tables
    for fixed_db_table in fixed_db_tables:
        # read table from qa
        with engine_qa.begin() as opened_engine_qa:
            df = pd.read_sql_table(fixed_db_table, opened_engine_qa)

        # update to local db
        with engine_local.begin() as opened_engine_local:
            try:
                df.to_sql(
                    name=fixed_db_table,
                    con=opened_engine_local,
                    index=False,
                    if_exists='replace'
                )
                print('updated table: '+fixed_db_table)
            except Exception as e:
                print('Error updating table {}'.format(fixed_db_table))


def get_engine_qa():
    qa_config = {
        "host": "52.16.7.222",
        "port": 3306,
        "database": "pheno20",
        "user": "phenome",
        "password": "fCK@MiV^84wP"}
    return sqlalchemy.create_engine(create_engine_string(**qa_config))


def create_engine_string(**kwargs):
    import urllib.parse
    host = kwargs['host']
    port = kwargs['port']
    database = kwargs['database']
    user = kwargs['user']
    password = kwargs['password']
    parsed_password = urllib.parse.quote_plus(password)
    return "mysql+pymysql://"+user+":"+parsed_password+"@"+host+":"+str(port)+"/"+database


# # https://stackoverflow.com/questions/30188796/connecting-to-mysql-database-via-ssh
# from sshtunnel import SSHTunnelForwarder
# from sqlalchemy import create_engine
#
# server =  SSHTunnelForwarder(
#     ('host', 22),
#     ssh_password="password",
#     ssh_username="username",
#     remote_bind_address=('127.0.0.1', 3306))
#
# server.start()
#
# engine = create_engine('mysql+mysqldb://user:pass@127.0.0.1:%s/db' % server.local_bind_port)
#
# # DO YOUR THINGS
#
# server.stop()