from timeit import default_timer as timer
from datetime import timedelta
from helper_functions import get_connections, update_users_actions, update_installations, main_loop_connections, \
    write_to_dest, simple_query_loop_connections, async_query_loop_connections
from configurations import connections_file_path, qa_config, unity_config, dest_config, queries
import asyncio


def update_db_account_management():
    flag_users_actions = False
    flag_installations = False
    flag_users = False
    flag_orgs = False
    flag_ual = False
    flag_simple_query = False
    flag_async_query = True

    connections = get_connections(connections_file_path)

    if flag_users_actions:
        query = queries["am_users_actions"]["query_string"]
        update_users_actions(qa_config, dest_config, query)

    if flag_installations:
        print('Updating installations table from {}'.format(unity_config["name"]))
        query = queries["installations"]["query_string"]
        update_installations(unity_config, dest_config, query)  # todo: unity config since qa doesn't have all installations

    if flag_users or flag_orgs or flag_ual:
        users, orgs, updated_max_ual = main_loop_connections(connections,
                                                             dest_config,
                                                             flag_users=flag_users,
                                                             flag_orgs=flag_orgs,
                                                             flag_ual=flag_ual)
        if flag_users:
            write_to_dest(users, dest_config, 'am_users')
        if flag_orgs:
            write_to_dest(orgs, dest_config, 'am_organizations')

    if flag_simple_query:
        query = queries["field_entities"]["query_string"]
        df = simple_query_loop_connections(connections, query, write_to_excel=True)

    if flag_async_query:
        # query = queries["field_entities"]["query_string"]
        query = """SELECT * from organizations """
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