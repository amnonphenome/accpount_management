import pandas
import sqlalchemy
import pandas as pd
from datetime import datetime
from configurations import queries
from pathlib import Path
from sshtunnel import SSHTunnelForwarder
import asyncio
import csv
import urllib.parse


def valid_connection(dictionary):
    required = {'host', 'port', 'user', 'password'}
    return required <= dictionary.keys()


def get_connections(file: str):
    """
    reads csv and returns list of dicts of connections config
    :param file:
    :return:
    """
    with open(file, encoding="utf-8-sig") as f:
        conns = [{k: v for k, v in row.items()}
                 for row in csv.DictReader(f, skipinitialspace=True)]
        return conns


def create_connections(conns):
    connections = []
    for conn in conns:
        if valid_connection(conn):
            connections.append(Connection(conn))
        else:
            print('Connection {} invalid'.format(conn["name"]))
            continue
    return connections


class Connection:

    connect = 1
    installation_id = 0
    name = 'Default'
    host = None
    port = None
    database = None
    user = None
    password = None
    ssh_host = None
    ssh_port = None
    ssh_user = None
    ssh_password = None
    is_ssh = False

    def __init__(self, dictionary):
        self.__dict__.update(dictionary)
        self.full_name = self.__repr__()
        if self.ssh_host != '' and self.ssh_host is not None:
            self.is_ssh = True
            self.ssh_tunnel = None
        self.engine = None

    def __enter__(self):
        return self.connect_to_src()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.disconnect_from_src()

    def __repr__(self):
        rep = '{}_{}'.format(self.name, self.installation_id)
        return rep

    def get_engine(self):
        parsed_password = urllib.parse.quote_plus(self.password)
        engine_string = "mysql+pymysql://{}:{}@{}:{}/{}" \
            .format(self.user, parsed_password, self.host, str(self.port), self.database)
        engine = sqlalchemy.create_engine(engine_string)
        return engine

    def connect_to_src(self):
        if self.is_ssh:
            ssh_tunnel = SSHTunnelForwarder(
                (self.ssh_host, int(self.ssh_port)),
                ssh_password=urllib.parse.quote_plus(self.ssh_password),
                ssh_username=self.ssh_user,
                remote_bind_address=(self.host, int(self.port)))
            try:
                ssh_tunnel.start()
                self.ssh_tunnel = ssh_tunnel
                self.port = ssh_tunnel.local_bind_port
                self.engine = self.get_engine()
                self.engine.begin()
                print('Connected to {} over ssh'.format(self.name))
                return self.engine
            except Exception as e:
                print('Connection over ssh failed: {}'.format(e))
                return None
        else:
            try:
                self.engine = self.get_engine()
                self.engine.begin()
                print('Connected to {}'.format(self.name))
                return self.engine
            except Exception as e:
                print('Connection to {} failed: {}'.format(self.full_name, e))
                return None

    def disconnect_from_src(self):
        self.engine.dispose()
        print('Disconnected from {}'.format(self.name))

    def get_data(self, query: str):
        try:
            with self as engine:
                if engine is not None:
                    print('Executing query in: {}'.format(self.full_name))
                    df = pd.read_sql_query(query, engine)
                    df['installation_id'] = self.installation_id
                    return df
                else:
                    return None
        except Exception as e:
            print('Query execution failed in {}, error: {}'.format(self.full_name, e))

    async def async_get_data(self, query: str):
        try:
            with self as engine:
                if engine is not None:
                    print('Executing query in: {}'.format(self.full_name))
                    df = pd.read_sql_query(query, engine)
                    df['installation_id'] = self.installation_id
                    return df
                else:
                    return None
        except Exception as e:
            print('Query execution failed in {}, error: {}'.format(self.full_name, e))


def update_users_actions(src_connection: Connection, dest_connection: Connection, query: str):
    df = pd.DataFrame()
    try:
        df = src_connection.get_data(query)
    except Exception as e:
        print('Connection to {} failed: {}'.format(src_connection.full_name, e))
    am_users_actions_table = "am_users_actions"
    try:
        print("Connecting to destination server: ", end=' ')
        dest_engine = dest_connection.connect_to_src()
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
            dest_connection.disconnect_from_src()
        except Exception as e:
            print('Error updating table {}: {}'.format(am_users_actions_table, e))
    except Exception as e:
        print('Failed, error: {}'.format(e))


def update_installations(src_connection: Connection, dest_connection: Connection, query: str):
    df = pd.DataFrame()
    try:
        df = src_connection.get_data(query)
    except Exception as e:
        print('Connection to {} failed: {}'.format(src_connection.full_name, e))
    table_name = "installations"
    print("Connecting to destination server: ", end=' ')
    try:
        print("Connecting to destination server: ", end=' ')
        dest_engine = dest_connection.connect_to_src()
        print('OK')
        try:
            print("Updating {}..".format(table_name), end=' ')
            df.to_sql(
                name=table_name,
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
                        '''.format(table_name))
            print('OK')
            dest_connection.disconnect_from_src()
        except Exception as e:
            print('Error updating table {}: {}'.format(table_name, e))
    except Exception as e:
        print('Failed, error: {}'.format(e))


def main_loop_connections(connections: list,
                          dest_connection: Connection,
                          flag_users=False,
                          flag_orgs=False,
                          flag_ual=False):
    """

    :param connections:
    :param dest_connection:
    :param flag_users:
    :param flag_orgs:
    :param flag_ual:
    :return:
    """

    max_ual = get_max_ual(dest_connection)
    users_query = queries["users"]["query_string"]
    orgs_query = queries["orgs"]["query_string"]
    users = pd.DataFrame()
    orgs = pd.DataFrame()
    updated_max_ual = {}

    for conn in connections:
        try:
            if flag_users:
                print('Getting users..')
                new_users = conn.get_data(users_query)
                users = pd.concat([users, new_users])
                print('Added ({} users)'.format(new_users.shape[0]))
            if flag_orgs:
                print('Getting organizations..')
                new_orgs = conn.get_data(orgs_query)
                orgs = pd.concat([orgs, new_orgs])
                print('Added ({} orgs)'.format(new_orgs.shape[0]))
            if flag_ual:
                i = 0
                ual_query = 'SELECT id, user_id, date, action_id FROM users_actions_log WHERE ' \
                            'action_id IN (SELECT id FROM users_actions where to_display=1) AND ' \
                            'user_id IN (SELECT id FROM users WHERE organization_id!=15) AND ' \
                            'id > {} and year(date)>2019'.format(max_ual[int(conn.installation_id)])
                chunk_size = 100000
                print('Copying to am_ual..')
                src_engine = conn.connect_to_src()
                dest_engine = dest_connection.connect_to_src()
                for chunk in pd.read_sql_query(ual_query, src_engine, chunksize=chunk_size):
                    chunk['installation_id'] = conn.installation_id
                    i += chunk.shape[0]
                    chunk.to_sql('am_ual', dest_engine, if_exists='append', schema='db_account_management', index=False)
                    print('{} rows copied'.format(i))
                try:
                    updated_max_ual = conn.get_data('SELECT max(id) FROM users_actions_log').iloc[0][0]
                    statement = "UPDATE installations SET max_ual={} WHERE id={};"\
                        .format(updated_max_ual, conn.installation_id)
                    print("Executing: {}".format(statement))
                    dest_connection.engine.execute(statement)
                except Exception as e:
                    print('Error updating installations table: {}'.format(e))
        except Exception as e:
            print('Execution failed, error: {}'.format(e))
    return users, orgs, updated_max_ual


def get_max_ual(dest_connection: Connection):
    """
    queries installations table from dest for max_ual
    :param dest_connection:
    :return: installation_id and max_ual as dict
    """
    query = "SELECT id, max_ual FROM installations WHERE max_ual IS NOT NULL"
    try:
        dest_engine = dest_connection.connect_to_src()
        print("Getting current max_ual from destination server: ", end=' ')
        df = pd.read_sql_query(query, dest_engine, index_col='id')
        print('OK')
        return df.to_dict('dict')['max_ual']
    except Exception as e:
        print('Query failed: {}'.format(e))


def write_to_dest(df: pandas.DataFrame, dest_connection: Connection, table_name: str):
    """

    :param df:
    :param dest_connection:
    :param table_name:
    :return:
    """
    df.reset_index(drop=True, inplace=True)
    with dest_connection.connect_to_src() as dest_engine:
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


async def async_query_loop_connections(connections: list, query: str, write_to_excel=True):
    """

    :param connections:
    :param query:
    :param write_to_excel:
    :return:
    """
    tasks = []
    replies = []
    for conn in connections:
        try:
            tasks.append(asyncio.create_task(conn.async_get_data(query)))
        except Exception as e:
            print('Connection to {} failed: {}'.format(conn.full_name, e))

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
