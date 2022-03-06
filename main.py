from timeit import default_timer as timer
from datetime import timedelta
from helper_functions import get_connections, update_users_actions, update_installations, main_loop_connections, \
    write_to_dest, async_query_loop_connections, create_connections, Connection
from configurations import connections_file_path, qa_config, unity_config, dest_config, queries
import asyncio


def update_db_account_management():
    flag_users_actions = False
    flag_installations = False
    flag_users = False
    flag_orgs = False
    flag_ual = True
    flag_async_query = False

    connections = [conn for conn in create_connections(get_connections(connections_file_path)) if conn.connect == '1']
    dest_connection = Connection(dest_config)
    qa_connection = Connection(qa_config)
    unity_connection = Connection(unity_config)

    if flag_users_actions:
        query = queries["am_users_actions"]["query_string"]
        update_users_actions(qa_connection, dest_connection, query)

    if flag_installations:
        print('Updating installations table from {}'.format(unity_connection.full_name))
        query = queries["installations"]["query_string"]
        update_installations(unity_connection, dest_connection, query)
        # todo: unity config since qa doesn't have all installations

    if flag_users or flag_orgs or flag_ual:
        users, orgs, updated_max_ual = main_loop_connections(connections,
                                                             dest_connection,
                                                             flag_users=flag_users,
                                                             flag_orgs=flag_orgs,
                                                             flag_ual=flag_ual)
        if flag_users:
            write_to_dest(users, dest_connection, 'am_users')
        if flag_orgs:
            write_to_dest(orgs, dest_connection, 'am_organizations')

    if flag_async_query:
        # query = queries["field_entities"]["query_string"]
        query = """SELECT * from organizations"""
        df = asyncio.run(async_query_loop_connections(connections, query, write_to_excel=True))
        print(df)


def run():
    start = timer()
    update_db_account_management()
    end = timer()
    print("time: {}".format(timedelta(seconds=end-start)))


if __name__ == "__main__":
    run()
    print('done')
