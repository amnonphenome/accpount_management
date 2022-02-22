import pandas
import sqlalchemy
import pandas as pd
from configurations import queries


def get_connections(connections_file_full_path: str):
    """
    gets connections details from csv
    :param connections_file_full_path:
    :return: connections_dict by installation_id as key
    """
    conns = pd.read_csv(connections_file_full_path)
    conns.loc[conns['connect'] == 1]
    conns_dict = conns.set_index('installation_id', drop=False).T.to_dict('dict')
    return conns_dict


def connect_over_ssh(ssh_config: dict, config: dict):
    """
    gets ssh and tcpip config dicts and connects to server
    :param ssh_config:
    :param config:
    :return: sqlalchemy_engine, ssh_server
    """
    print('Unable to connect over SSH yet')
    engine = 'Unable to connect over SSH yet'
    return engine  # todo: make work
    # from sshtunnel import SSHTunnelForwarder
    # ssh_server = SSHTunnelForwarder(
    #     (ssh_config['ssh_host'], ssh_config['port']),
    #     ssh_password=ssh_config["password"],
    #     ssh_username=ssh_config["username"],
    #     remote_bind_address=(config['host'], config['port']))
    # # server.start()
    # config['port'] = ssh_server.local_bind_port
    # engine = sqlalchemy.create_engine(create_engine_string(config))
    # return engine, ssh_server

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


def update_users_actions(src_config: dict, dest_config: dict, query: str):
    # read:
    src_engine = get_engine(src_config)
    try:
        print('Connecting to {}: '.format(src_config["name"]), end=' ')
        with src_engine.begin() as src_engine:
            print('OK')
            try:
                df = pd.read_sql_query(query, src_engine, index_col='id')
            except Exception as e:
                print('Query failed: {}'.format(e))
    except Exception as e:
        print('Failed, error: {}'.format(e))
    # write:
    dest_engine = get_engine_tcpip(dest_config)
    am_users_actions_table = "am_users_actions"
    print("Connecting to destination server: ", end=' ')
    try:
        with dest_engine.begin() as dest_engine:
            print('OK')
            try:
                print("Updating {}..".format(am_users_actions_table), end=' ')
                df.to_sql(
                    name=am_users_actions_table,
                    con=dest_engine,
                    index=True,
                    if_exists='replace',
                    dtype={'id': sqlalchemy.types.INT,
                           'name': sqlalchemy.types.VARCHAR(length=255)
                           }
                )
                # # Fix the pk back: todo: why is this happening?
                dest_engine.execute('''
                            ALTER TABLE `db_account_management`.{}
                            CHANGE COLUMN `id` `id` INT NOT NULL AUTO_INCREMENT ,
                            ADD PRIMARY KEY (`id`);
                            '''.format(am_users_actions_table))
                print('OK')
            except Exception as e:
                print('Error updating table {}: {}'.format(am_users_actions_table, e))
    except Exception as e:
        print('Failed, error: {}'.format(e))


def update_installations(src_config: dict, dest_config: dict, query: str):
    # read:
    src_engine = get_engine(src_config)
    try:
        print('Connecting to {}: '.format(src_config["name"]), end=' ')
        with src_engine.begin() as src_engine:
            print('OK')
            try:
                df = pd.read_sql_query(query, src_engine, index_col='id')
                df['max_ual'] = None
            except Exception as e:
                print('Query failed: {}'.format(e))
    except Exception as e:
        print('Failed, error: {}'.format(e))
    # write:
    dest_engine = get_engine_tcpip(dest_config)
    table_name = "installations"
    print("Connecting to destination server: ", end=' ')
    try:
        with dest_engine.begin() as dest_engine:
            print('OK')
            try:
                print("Updating {}..".format(table_name), end=' ')
                df.to_sql(
                    name=table_name,
                    con=dest_engine,
                    index=True,
                    if_exists='replace',
                    dtype={'id': sqlalchemy.types.INT,
                           'name': sqlalchemy.types.VARCHAR(length=45)
                           }
                )
                # # Fix the pk back: todo: why is this happening?
                dest_engine.execute('''
                            ALTER TABLE `db_account_management`.{}
                            CHANGE COLUMN `id` `id` INT NOT NULL AUTO_INCREMENT ,
                            ADD PRIMARY KEY (`id`);
                            '''.format(table_name))
                print('OK')
            except Exception as e:
                print('Error updating table {}: {}'.format(table_name, e))
    except Exception as e:
        print('Failed, error: {}'.format(e))


def get_engine(conn_config: dict):
    connection_keys = conn_config.keys()
    tcpip_config = {key: conn_config[key] for key in connection_keys & {'host', 'port', 'database', 'user', 'password',
                                                                        'name'}}

    if isinstance(conn_config['ssh_host'], str):
        ssh_config = {key: conn_config[key] for key in connection_keys & {'ssh_host', 'ssh_port', 'ssh_user',
                                                                          'ssh_password'}}
        engine = connect_over_ssh(ssh_config, tcpip_config)
    else:
        engine = get_engine_tcpip(tcpip_config)
    return engine


def get_engine_tcpip(config: dict):
    """
    tries to connect (tcpip) using config
    :param config:
    :return: engine or error
    """
    import urllib.parse
    host = config['host']
    port = config['port']
    database = config['database']
    user = config['user']
    password = config['password']
    parsed_password = urllib.parse.quote_plus(password)
    engine_string = "mysql+pymysql://" + user + ":" + parsed_password + "@" + host + ":" + str(port) + "/" + database
    engine = sqlalchemy.create_engine(engine_string)
    return engine


def main_loop_connections(connections: dict, dest_config: dict, flag_users=False, flag_orgs=False, flag_ual=False):
    """

    :param connections:
    :param dest_config:
    :param flag_users:
    :param flag_orgs:
    :param flag_ual:
    :return:
    """

    max_ual = get_max_ual(dest_config)
    users_query = queries["users"]["query_string"]
    orgs_query = queries["orgs"]["query_string"]
    orgs = pd.DataFrame()
    users = pd.DataFrame()
    installations = connections.keys()
    dest_engine = get_engine_tcpip(dest_config)
    try:
        print('Connecting to destination server: ', end=' ')
        with dest_engine.begin() as dest_engine:
            print('OK')
            for installation in installations:
                installation_id = installation['installation_id']
                src_config = installation[installation_id]
                # get engine for current installation
                src_engine = get_engine(src_config)
                try:
                    print('Connecting to {}: '.format(src_config["name"]), end=' ')
                    with src_engine.begin() as src_engine:
                        print('OK')
                        try:
                            if flag_orgs:
                                print('Getting organizations..')
                                new_orgs = pd.read_sql_query(orgs_query, src_engine, index_col='id')
                                orgs = pd.concat([orgs, new_orgs])
                                print('OK ({} orgs)'.format(new_orgs.shape[0]))
                            if flag_users:
                                print('Getting users..')
                                new_users = pd.read_sql_query(users_query, src_engine, index_col='id')
                                users = pd.concat([users, new_users])
                                print('OK ({} users)'.format(new_users.shape[0]))
                            if flag_ual:
                                i = 0
                                ual_query = 'SELECT id, user_id, date, action_id FROM pheno20.users_actions_log where id > {}'.format(max_ual[installation_id])
                                chunk_size = 100000
                                for chunk in pd.read_sql_query(ual_query, src_engine, chunksize=chunk_size):
                                    chunk['installation_id'] = installation_id
                                    i += chunk.shape[0]
                                    chunk.to_sql('am_ual', dest_engine,
                                                 if_exists='append',
                                                 schema='db_account_management',
                                                 index=False)
                                    print('{} rows copied'.format(i))

                        except Exception as e:
                            print('Execution failed, error: {}'.format(e))
                except Exception as e:
                    print('Failed1, error: {}'.format(e))
    except Exception as e:
        print('Failed2, error: {}'.format(e))





# def update_fixed_tables(src_engine: str, dest_engine: str, tables_list: list):
#     """
#     copies list of tables from src to dest
#     :param src_engine:
#     :param dest_engine:
#     :param tables_list:
#     :return: success or error (str)
#     """
#     for fixed_db_table in tables_list:
#         # read table from qa
#         with src_engine.begin() as src_engine:
#             print('src_engine: {}'.format(src_engine))
#             df = pd.read_sql_table(fixed_db_table, src_engine, index_col='id')
#             # add column max_ual if writing installations
#             if fixed_db_table == "installations":
#                 df['max_ual'] = 0
#         # update this table to local db
#         with dest_engine.begin() as dest_engine:
#             print('dest_engine: {}'.format(dest_engine))
#             try:
#                 df.to_sql(
#                     name=fixed_db_table,
#                     con=dest_engine,
#                     index=True,
#                     if_exists='replace',
#                     dtype={'id': sqlalchemy.types.INT,
#                            'name': sqlalchemy.types.VARCHAR(length=255)
#                            }
#                 )
#                 # Fix the pk back: todo: why is this happening?
#                 dest_engine.execute('''
#                     ALTER TABLE `db_account_management`.{}
#                     CHANGE COLUMN `id` `id` INT NOT NULL AUTO_INCREMENT ,
#                     ADD PRIMARY KEY (`id`);
#                     '''.format(fixed_db_table))
#                 print('updated table: ' + fixed_db_table)
#             except Exception as e:
#                 print('Error updating table {}: {}'.format(fixed_db_table, e))
#         # dest_engine.close()


def get_max_ual(dest_config: dict):
    """
    queries installations table from dest for max_ual
    :param dest_config:
    :return: installation_id and max_ual as dict
    """
    # local db_account_management engine
    dest_engine = get_engine_tcpip(dest_config)
    table_name = "installations"
    print("Connecting to destination server: ", end=' ')
    query = "SELECT id, max_ual FROM installations WHERE max_ual IS NOT NULL"
    try:
        with dest_engine.begin() as dest_engine:
            print('OK')
            df = pd.read_sql_query(query, dest_engine, index_col='id')
        return df.to_dict('dict')['max_ual']
    except Exception as e:
        print('Query failed: {}'.format(e))


# def get_engine_per_connection(conn_config: dict):
#     """
#     loops through connections, gets queries using the max_ual
#     :param conn_config: (dict)
#     :return: engine: (str)
#     """
#     connection_keys = conn_config.keys()
#     # define connection dicts
#     db = {key: conn_config[key] for key in connection_keys & {'host', 'port', 'database', 'user', 'password', 'name'}}
#     ssh = {key: conn_config[key] for key in connection_keys & {'ssh_host', 'ssh_port', 'ssh_user', 'ssh_password'}}
#     ssh_server = []
#     src_engine = []
#     if isinstance(conn_config['ssh_host'], str):
#         print(f'getting connection engine for: {conn_config["name"]} over ssh ... ')
#         try:
#             src_engine, ssh_server = get_engine_over_ssh(ssh, db)
#         except Exception:
#             print('Connection failed')
#     else:
#         print(f'getting connection engine for: {conn_config["name"]} ... ')
#         try:
#             src_engine = get_engine_tcpip(db)
#         except Exception:
#             print('Connection failed')
#
#     return src_engine, ssh_server
#
#
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
#
#
# def get_organizations_from_installation(src_engine, installation_id: int):
#     """
#     returns and appends organizations from this installation_id
#     :param src_engine:
#     :param installation_id:
#     :return:
#     """
#     sql = queries["orgs"]["query_string"]
#     with src_engine.begin() as src_engine:
#         df = pd.read_sql_query(sql, src_engine)
#         df['installation_id'] = installation_id
#         print('found {} organizations'.format(df.shape[0]))
#     return df
#
#
# def get_users_from_installation(src_engine, installation_id: int):
#     """
#     returns and appends organizations from this installation_id
#     :param src_engine:
#     :param installation_id:
#     :return:
#     """
#     sql = queries["users"]["query_string"]
#     with src_engine.begin() as src_engine:
#         df = pd.read_sql_query(sql, src_engine)
#         df['installation_id'] = installation_id
#         print('found {} users'.format(df.shape[0]))
#     return df
#
#
# def write_table_to_dest(df: pandas.DataFrame, dest_engine, table_name: str):
#     """
#     :param table_name:
#     :param df:
#     :param dest_engine:
#     :return:
#     """
#     with dest_engine.begin() as dest_engine:
#         try:
#             df.to_sql(
#                 name=table_name,
#                 con=dest_engine,
#                 index=True,
#                 if_exists='replace'
#             )
#             dest_engine.execute('''
#                     ALTER TABLE `db_account_management`.{}
#                     CHANGE COLUMN `index` `index` INT NOT NULL ,
#                     ADD PRIMARY KEY (`index`);
#                     '''.format(table_name))
#             print('updated table: ' + table_name)
#         except Exception as e:
#             print('Error updating table {}: {}'.format(table_name, e))


# def create_engine_string(config: dict):
#     """
#     Takes connection parameters as dict and returns sqlalchemy engine string
#     Parameters:
#         config (dict)  : connection parameters
#     Returns:
#         str : engine_string
#     """
#     import urllib.parse
#     host = config['host']
#     port = config['port']
#     database = config['database']
#     user = config['user']
#     password = config['password']
#     parsed_password = urllib.parse.quote_plus(password)
#     engine_string = "mysql+pymysql://" + user + ":" + parsed_password + "@" + host + ":" + str(port) + "/" + database
#     # print("engine_string: {}".format(engine_string))
#     return engine_string


# def get_engine_tcpip(config: dict):
#     """
#     takes dict of connection returns sqlalchemy engine
#     :type config: (dict)
#     :param config: (dict)
#     :return: engine (str)
#     """
#     engine = sqlalchemy.create_engine(create_engine_string(config))
#     return engine