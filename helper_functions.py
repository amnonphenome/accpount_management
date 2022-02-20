import pandas
import sqlalchemy
import pandas as pd
from configurations import queries


def get_connections(connections_file_full_path):
    """
    gets connections details from csv
    :param connections_file_full_path:
    :return: connections_list
    """
    conns = pd.read_csv(connections_file_full_path).to_dict('records')
    return [connection for connection in conns if connection['connect']]


def get_engine_tcpip(config: dict):
    """
    takes dict of connection returns sqlalchemy engine
    :type config: (dict)
    :param config: (dict)
    :return: engine (str)
    """
    engine = sqlalchemy.create_engine(create_engine_string(config))
    return engine


def get_engine_over_ssh(ssh_config: dict, config: dict):
    """
    gets ssh and regular config dicts returns engine and ssh_server that should be opened and closed
    :param ssh_config:
    :param config:
    :return: sqlalchemy_engine, ssh_server
    """
    from sshtunnel import SSHTunnelForwarder  # todo: hasn't been tested yet
    ssh_server = SSHTunnelForwarder(
        (ssh_config['ssh_host'], ssh_config['port']),
        ssh_password=ssh_config["password"],
        ssh_username=ssh_config["username"],
        remote_bind_address=(config['host'], config['port']))
    # server.start()
    config['port'] = ssh_server.local_bind_port
    engine = sqlalchemy.create_engine(create_engine_string(config))
    return engine, ssh_server

    # Example from: https://stackoverflow.com/questions/30188796/connecting-to-mysql-database-via-ssh
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


def create_engine_string(config: dict):
    """
    Takes connection parameters as dict and returns sqlalchemy engine string
    Parameters:
        config (dict)  : connection parameters
    Returns:
        str : engine_string
    """
    import urllib.parse
    host = config['host']
    port = config['port']
    database = config['database']
    user = config['user']
    password = config['password']
    parsed_password = urllib.parse.quote_plus(password)
    engine_string = "mysql+pymysql://"+user+":"+parsed_password+"@"+host+":"+str(port)+"/"+database
    # print("engine_string: {}".format(engine_string))
    return engine_string


def update_fixed_tables(src_engine: str, dest_engine: str, tables_list: list):
    """
    copies list of tables from src to dest
    :param src_engine:
    :param dest_engine:
    :param tables_list:
    :return: success or error (str)
    """

# read>update for all fixed tables
    for fixed_db_table in tables_list:
        # read table from qa
        with src_engine.begin() as src_engine:
            print('src_engine: {}'.format(src_engine))
            df = pd.read_sql_table(fixed_db_table, src_engine, index_col='id')

        # update this table to local db
        with dest_engine.begin() as dest_engine:
            print('dest_engine: {}'.format(dest_engine))
            try:
                df.to_sql(
                    name=fixed_db_table,
                    con=dest_engine,
                    index=True,
                    if_exists='replace',
                    dtype={'id': sqlalchemy.types.INT,
                           'name': sqlalchemy.types.VARCHAR(length=255)
                           }
                    )
                # Fix the pk back: todo: why is this happening?
                dest_engine.execute('''
                    ALTER TABLE `db_account_management`.{}
                    CHANGE COLUMN `id` `id` INT NOT NULL AUTO_INCREMENT ,
                    ADD PRIMARY KEY (`id`);
                    '''.format(fixed_db_table))
                print('updated table: '+fixed_db_table)
            except Exception as e:
                print('Error updating table {}: {}'.format(fixed_db_table, e))
        # dest_engine.close()


def get_max_ual(dest_engine):
    """
    queries installations table from dest for max_ual
    :param dest_engine:
    :return: installation_id and max_ual as dict
    """
    # local db_account_management engine
    dest_db_engine = get_engine_tcpip(dest_engine)
    print('dest_engine: Connected to {} on {}'.format(dest_db_engine.url.database, dest_db_engine.url.host))
    query = "SELECT id, max_ual FROM installations"
    with dest_db_engine.begin() as opened_engine_dest:
        df = pd.read_sql_query(query, opened_engine_dest, index_col='id')
    return df.to_dict('dict')['max_ual']


def get_engine_per_connection(conn_config: dict):
    """
    loops through connections, gets queries using the max_ual
    :param conn_config: (dict)
    :return: engine: (str)
    """
    connection_keys = conn_config.keys()
    # define connection dicts
    db = {key: conn_config[key] for key in connection_keys & {'host', 'port', 'database', 'user', 'password', 'name'}}
    ssh = {key: conn_config[key] for key in connection_keys & {'ssh_host', 'ssh_port', 'ssh_user', 'ssh_password'}}
    ssh_server = []
    src_engine = []
    if isinstance(conn_config['ssh_host'], str):
        print(f'getting connection engine for: {conn_config["name"]} over ssh ... ')
        try:
            src_engine, ssh_server = get_engine_over_ssh(ssh, db)
        except Exception:
            print('Connection failed')
    else:
        print(f'getting connection engine for: {conn_config["name"]} ... ')
        try:
            src_engine = get_engine_tcpip(db)
        except Exception:
            print('Connection failed')

    return src_engine, ssh_server


def read_write_ual_to_dest(src_engine, dest_engine, installation_id: int, chunk_size=100000):
    """
    gets max_ual from dest, reads_writes from ual to dest
    :param src_engine:
    :param dest_engine:
    :param installation_id:
    :param chunk_size:
    :return:
    """
    max_ual = pd.read_sql_query("SELECT max(id) FROM db_account_management.am_ual where installation_id={}"
                                .format(installation_id), dest_engine).iloc[0]['max(id)']
    if max_ual is None:
        max_ual = 0
    print('max_ual: {}'.format(max_ual))
    sql = "SELECT id, user_id, date, action_id FROM pheno20.users_actions_log where id > {}".format(max_ual)
    i = 0
    with dest_engine.begin() as dest_engine:
        with src_engine.begin() as src_engine:
            for chunk in pd.read_sql_query(sql, src_engine, chunksize=chunk_size):
                chunk['installation_id'] = installation_id
                i += chunk.shape[0]
                chunk.to_sql('am_ual', dest_engine, if_exists='append', schema='db_account_management', index=False)
                print('{} rows copied'.format(i))


def get_organizations_from_installation(src_engine, installation_id: int):
    """
    returns and appends organizations from this installation_id
    :param src_engine:
    :param installation_id:
    :return:
    """
    sql = queries["orgs"]["query_string"]
    with src_engine.begin() as src_engine:
        df = pd.read_sql_query(sql, src_engine)
        df['installation_id'] = installation_id
        print('found {} organizations'.format(df.shape[0]))
    return df


def get_users_from_installation(src_engine, installation_id: int):
    """
    returns and appends organizations from this installation_id
    :param src_engine:
    :param installation_id:
    :return:
    """
    sql = queries["users"]["query_string"]
    with src_engine.begin() as src_engine:
        df = pd.read_sql_query(sql, src_engine)
        df['installation_id'] = installation_id
        print('found {} users'.format(df.shape[0]))
    return df


def write_table_to_dest(df: pandas.DataFrame, dest_engine, table_name: str):
    """
    :param table_name:
    :param df:
    :param dest_engine:
    :return:
    """
    with dest_engine.begin() as dest_engine:
        try:
            df.to_sql(
                name=table_name,
                con=dest_engine,
                index=True,
                if_exists='replace'
            )
            dest_engine.execute('''
                    ALTER TABLE `db_account_management`.{}
                    CHANGE COLUMN `index` `index` INT NOT NULL ,
                    ADD PRIMARY KEY (`index`);
                    '''.format(table_name))
            print('updated table: '+table_name)
        except Exception as e:
            print('Error updating table {}: {}'.format(table_name, e))


