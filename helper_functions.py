import sqlalchemy
import pandas as pd


def get_connections(connections_file):
    """
    gets connections details from csv
    :param connections_file:
    :return: connections_list
    """
    conns = pd.read_csv(connections_file).to_dict('records')
    return [connection for connection in conns if connection['connect']]


def get_engine(config: dict):
    """
    takes dict of connection returns sqlalchemy engine
    :param config: (dict)
    :return: engine (str)
    """
    engine = sqlalchemy.create_engine(create_engine_string(config))
    # print('Dest engine: Connected to {} on {}'.format(dest_engine.url.database, dest_engine.url.host))
    return engine


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

    # qa server engine
    src_db_engine_qa = get_src_engine()
    print('src_engine: Connected to {} on {}'.format(src_db_engine_qa.url.database, src_db_engine_qa.url.host))

    # local db_account_management engine
    dest_db_engine = get_engine()

    # read>update for all fixed tables
    for fixed_db_table in tables_list:
        # read table from qa
        with src_db_engine_qa.begin() as opened_engine_qa:
            df = pd.read_sql_table(fixed_db_table, opened_engine_qa, index_col='id')

        # update this table to local db
        with dest_db_engine.begin() as opened_engine_local:
            try:
                df.to_sql(
                    name=fixed_db_table,
                    con=opened_engine_local,
                    index=True,
                    if_exists='replace',
                    dtype={'id': sqlalchemy.types.INT,
                           'name': sqlalchemy.types.VARCHAR(length=255)
                           }
                    )
                opened_engine_local.execute('''
                    ALTER TABLE `db_account_management`.{}
                    CHANGE COLUMN `id` `id` INT NOT NULL AUTO_INCREMENT ,
                    ADD PRIMARY KEY (`id`);
                    '''.format(fixed_db_table))
                print('updated table: '+fixed_db_table)
            except Exception as e:
                print('Error updating table {}: {}'.format(fixed_db_table, e))
        # opened_engine_local.close()


def get_engine_over_ssh(ssh_config: dict, config: dict):
    """
    gets ssh and regular config dicts returns engine and ssh_server that should be opened and closed
    :param ssh_config:
    :param config:
    :return: sqlalchemy_engine, ssh_server
    """
    from sshtunnel import SSHTunnelForwarder  # todo: hasn't been tested yet
    from sqlalchemy import create_engine
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