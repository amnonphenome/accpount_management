import pandas
import sqlalchemy
import pandas as pd
from datetime import datetime
from configurations import queries
from pathlib import Path
from sshtunnel import SSHTunnelForwarder
import asyncio


def get_connections(connections_file_full_path: str):
    """
    gets connections details from csv
    :param connections_file_full_path:
    :return: connections_dict by installation_id as key
    """
    conns = pd.read_csv(connections_file_full_path)
    conns = conns.loc[conns['connect'] == 1]
    conns_dict = conns.set_index('installation_id', drop=False).to_dict('index')
    return conns_dict


def connect_over_ssh(ssh_config: dict, config: dict):
    """
    gets ssh and tcpip config dicts and connects to server
    :param ssh_config:
    :param config:
    :return: sqlalchemy_engine, ssh_server
    """
    print('Opening SSH tunnel:', end=' ')
    ssh_server = SSHTunnelForwarder(
        (ssh_config['ssh_host'], int(ssh_config['ssh_port'])),
        ssh_password=ssh_config["ssh_password"],
        ssh_username=ssh_config["ssh_user"],
        remote_bind_address=(config['host'],
                             config['port']))
    try:
        ssh_server.start()
        config['port'] = ssh_server.local_bind_port
        engine = get_engine_tcpip(config)
    except Exception as e:
        print('Connection failed: {}'.format(e))
        return None, None
    return engine, ssh_server


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
        engine, ssh_engine = connect_over_ssh(ssh_config, tcpip_config)
    else:
        engine = get_engine_tcpip(tcpip_config)
        ssh_engine = None
    return engine, ssh_engine


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
    loops through connections
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
    users = pd.DataFrame()
    orgs = pd.DataFrame()
    updated_max_ual = {}

    installation_ids = connections.keys()
    dest_engine = get_engine_tcpip(dest_config)
    try:
        print('Connecting to destination server: ', end=' ')
        with dest_engine.begin() as dest_engine:
            print('OK')
            for installation_id in installation_ids:
                src_config = connections[installation_id]
                # get engine for current installation
                src_engine = get_engine(src_config)
                try:
                    print('Connecting to installation_id: {} - {}:'
                          .format(src_config["installation_id"],
                                  src_config["name"]),
                          end=' ')
                    with src_engine.begin() as src_engine:
                        print('OK')
                        try:
                            if flag_users:
                                print('Getting users..')
                                new_users = pd.read_sql_query(users_query, src_engine)
                                new_users["installation_id"] = installation_id
                                users = pd.concat([users, new_users])
                                print('OK ({} users)'.format(new_users.shape[0]))
                            if flag_orgs:
                                print('Getting organizations..')
                                new_orgs = pd.read_sql_query(orgs_query, src_engine)
                                new_orgs["installation_id"] = installation_id
                                orgs = pd.concat([orgs, new_orgs])
                                print('OK ({} orgs)'.format(new_orgs.shape[0]))
                            if flag_ual:
                                i = 0
                                ual_query = 'SELECT id, user_id, date, action_id FROM users_actions_log WHERE ' \
                                            'action_id IN (SELECT id FROM users_actions where to_display=1) AND ' \
                                            'user_id IN (SELECT id FROM users WHERE organization_id!=15) AND ' \
                                            'id > {} and year(date)>2019'.format(max_ual[installation_id])
                                chunk_size = 100000
                                print(ual_query)
                                print('Copying to am_ual..')
                                for chunk in pd.read_sql_query(ual_query, src_engine, chunksize=chunk_size):
                                    chunk['installation_id'] = installation_id
                                    i += chunk.shape[0]
                                    chunk.to_sql('am_ual', dest_engine,
                                                 if_exists='append',
                                                 schema='db_account_management',
                                                 index=False)
                                    print('{} rows copied'.format(i))
                                try:
                                    updated_max_ual = pd.read_sql_query('SELECT max(id) FROM users_actions_log',
                                                                        src_engine).iloc[0][0]
                                    statement = "UPDATE installations SET max_ual={} WHERE id={};"\
                                        .format(updated_max_ual, installation_id)
                                    print("Executing: {}".format(statement))
                                    dest_engine.execute(statement)
                                except Exception as e:
                                    print('Error updating installations table: {}'.format(e))
                        except Exception as e:
                            print('Execution failed, error: {}'.format(e))
                except Exception as e:
                    print('Failed1, error: {}'.format(e))
    except Exception as e:
        print('Failed2, error: {}'.format(e))
    return users, orgs, updated_max_ual


def get_max_ual(dest_config: dict):
    """
    queries installations table from dest for max_ual
    :param dest_config:
    :return: installation_id and max_ual as dict
    """
    # local db_account_management engine
    dest_engine = get_engine_tcpip(dest_config)
    print("Getting current max_ual from destination server: ", end=' ')
    query = "SELECT id, max_ual FROM installations WHERE max_ual IS NOT NULL"
    try:
        with dest_engine.begin() as dest_engine:
            print('OK')
            df = pd.read_sql_query(query, dest_engine, index_col='id')
        return df.to_dict('dict')['max_ual']
    except Exception as e:
        print('Query failed: {}'.format(e))


def write_to_dest(df: pandas.DataFrame, dest_config: dict, table_name: str):
    """

    :param df:
    :param dest_config:
    :param table_name:
    :return:
    """
    df.reset_index(drop=True, inplace=True)
    dest_engine = get_engine_tcpip(dest_config)
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
            print('updated table: ' + table_name)
        except Exception as e:
            print('Error updating table {}: {}'.format(table_name, e))


def simple_query_loop_connections(connections: dict, query: str, write_to_excel=True):
    """

    :param connections:
    :param query:
    :param write_to_excel:
    :return:
    """
    df = pd.DataFrame()
    installation_ids = connections.keys()
    for installation_id in installation_ids:
        src_config = connections[installation_id]
        # get engine for current installation
        src_engine = get_engine(src_config)
        try:
            print('Connecting to installation_id: {} - {}:'
                  .format(src_config["installation_id"],
                          src_config["name"]),
                  end=' ')
            with src_engine.begin() as src_engine:
                print('OK')
                try:
                    print('Executing: {}'.format(query))
                    new_rows = pd.read_sql_query(query, src_engine)
                    new_rows['installation_id'] = installation_id
                    df = pd.concat([df, new_rows])
                    print('new rows: \n{})'.format(new_rows.head(5)))
                except Exception as e:
                    print('Query execution failed, error: {}'.format(e))
        except Exception as e:
            print('Connection failed, error: {}'.format(e))
    if write_to_excel:
        now = datetime.now()
        dt_string = now.strftime("%y%m%d_%H%M%S")
        file_name = "output_"+dt_string+".xlsx"
        path = Path("output", file_name)
        df_list = [df]
        save_xls(df_list, path)
    return df


def save_xls(list_dfs, _path, sheets_names=None):
    # query names should match with list_dfs - sheets will be named accordingly
    if sheets_names is None:
        sheets_names = []
    with pd.ExcelWriter(_path,
                        engine='xlsxwriter',
                        engine_kwargs={'options': {'strings_to_formulas': False}}) as writer:
        for n, df in enumerate(list_dfs):
            if len(sheets_names) > 0:
                df.to_excel(writer, sheets_names[n])
            else:
                df.to_excel(writer, 'sheet%s' % n)
        # writer.save()
    print('output.xlsx path: {}'.format(_path))


async def fetch_data_async(installation_id, src_config, query):
    src_engine, ssh_engine = get_engine(src_config)
    if src_engine is not None:
        try:
            print('Executing query in installation_id: {} - {}'
                  .format(src_config["installation_id"],
                          src_config["name"]))
            with src_engine.begin() as src_engine:
                try:
                    # print('Executing in installation_id {}'.format(installation_id))
                    df = pd.read_sql_query(query, src_engine)
                    df['installation_id'] = installation_id
                    return df
                except Exception as e:
                    print('Query execution failed, error: {}'.format(e))
        except Exception as e:
            print('Connection failed, error: {}'.format(e))
            return None
    else:
        return None


async def async_query_loop_connections(connections: dict, query: str, write_to_excel=True):
    """

    :param connections:
    :param query:
    :param write_to_excel:
    :return:
    """
    installation_ids = connections.keys()
    tasks = []
    replies = []
    for installation_id in installation_ids:
        src_config = connections[installation_id]
        tasks.append(asyncio.create_task(fetch_data_async(installation_id, src_config, query)))
    for task in tasks:
        reply = await task
        if reply is not None:
            replies.append(reply)
    if replies:
        df = pd.concat(replies)

        if write_to_excel:
            now = datetime.now()
            dt_string = now.strftime("%y%m%d_%H%M%S")
            file_name = "output_"+dt_string+".xlsx"
            path = Path("output", file_name)
            df_list = [df]
            save_xls(df_list, path)
        return df


exec(open("main.py").read())
