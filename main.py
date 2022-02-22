from timeit import default_timer as timer
from datetime import timedelta
from helper_functions import get_connections, update_users_actions, update_installations, main_loop_connections
from configurations import connections_file_path, qa_config, unity_config, dest_config, queries
import pandas as pd

start = timer()

# Flags:
flag_users_actions = False
flag_installation = False
flag_users = False
flag_orgs = False
flag_ual = True

# get connections dict by installation_id
connections = get_connections(connections_file_path)

if flag_users_actions:
    query = queries["am_users_actions"]["query_string"]
    update_users_actions(qa_config, dest_config, query)

if flag_installation:
    print('Updating installations table from {}'.format(unity_config["name"]))
    query = queries["installations"]["query_string"]
    update_installations(unity_config, dest_config, query)  # todo: unity config since qa doesn't have all installations

if flag_users or flag_orgs or flag_ual:

    main_loop_connections(connections, dest_config, flag_users=flag_users, flag_orgs=flag_orgs, flag_ual=flag_ual)

# main loop of connections
# orgs = pd.DataFrame()
# users = pd.DataFrame()
# for conn_num, src_config in enumerate(connections):
#     installation_id = src_config['installation_id']
#     # get engine
#     src_engine, ssh_server = get_engine_per_connection(src_config)
#     # ssh engine
#     if ssh_server:
#         pass  # todo: ssh open ssh_tunnel
#     if update_users_and_orgs:
#         new_orgs = get_organizations_from_installation(src_engine, installation_id)
#         orgs = pd.concat([orgs, new_orgs])
#         new_users = get_users_from_installation(src_engine, installation_id)
#         users = pd.concat([users, new_users])
#     if update_ual:
#         read_write_ual_to_dest(src_engine, dest_engine, installation_id)
#
# if update_users_and_orgs:
#     orgs.reset_index(drop=True, inplace=True)
#     users.reset_index(drop=True, inplace=True)
#     write_table_to_dest(orgs, dest_engine, 'am_organizations')
#     write_table_to_dest(users, dest_engine, 'am_users')

end = timer()
print("time: {}".format(timedelta(seconds=end-start)))

