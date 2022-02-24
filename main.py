from timeit import default_timer as timer
from datetime import timedelta
from helper_functions import get_connections, update_users_actions, update_installations, main_loop_connections, \
    write_to_dest, update_max_ual
from configurations import connections_file_path, qa_config, unity_config, dest_config, queries


def update_db_account_management():
    # Flags:
    flag_users_actions = False
    flag_installations = False
    flag_users = False
    flag_orgs = False
    flag_ual = True

    # get connections dict by installation_id
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
        if flag_ual:
            update_max_ual(updated_max_ual, dest_config)


def run():
    start = timer()
    update_db_account_management()
    end = timer()
    print("time: {}".format(timedelta(seconds=end-start)))


if __name__ == "__main__":
    run()

