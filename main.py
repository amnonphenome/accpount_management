from timeit import default_timer as timer
from datetime import timedelta
from helper_functions import get_connections, update_fixed_tables, get_max_ual, get_engine_per_connection, \
    get_engine_tcpip, read_write_ual_to_dest, get_organizations_from_installation, get_users_from_installation, \
    write_table_to_dest
from configurations import dest_config, connections_file_path, qa_config, unity_config
import pandas as pd

start = timer()

# Flags:
fixed_tables_update = False
update_users_and_orgs = False
update_ual = True

#   define dest = db_account_management:
dest_engine = get_engine_tcpip(dest_config)

# get_connections
connections = get_connections(connections_file_path)

# db initiate (at first and at some version updates)
if fixed_tables_update:
    #   define src = qa/unity:
    src_engine = get_engine_tcpip(qa_config)
    fixed_tables_to_update = ["users_actions",
                              "users_actions_categories",
                              "application_modules",
                              "users_actions_types",
                              "installations"]
    update_fixed_tables(src_engine, dest_engine, fixed_tables_to_update)

# get max_ual from db_account_management
# max_ual_all = get_max_ual(dest_config)

# Get data from connections:
# main loop of connections
orgs = pd.DataFrame()
users = pd.DataFrame()
for conn_num, src_config in enumerate(connections):
    installation_id = src_config['installation_id']
    # get engine
    src_engine, ssh_server = get_engine_per_connection(src_config)
        # ssh engine
    if ssh_server:
        pass  # todo: ssh open ssh_tunnel
    if update_users_and_orgs:
        new_orgs = get_organizations_from_installation(src_engine, installation_id)
        orgs = pd.concat([orgs, new_orgs])
        new_users = get_users_from_installation(src_engine, installation_id)
        users = pd.concat([users, new_users])
    if update_ual:
        read_write_ual_to_dest(src_engine, dest_engine, installation_id)

if update_users_and_orgs:
    orgs.reset_index(drop=True, inplace=True)
    users.reset_index(drop=True, inplace=True)
    write_table_to_dest(orgs, dest_engine, 'am_organizations')
    write_table_to_dest(users, dest_engine, 'am_users')

end = timer()
print("time: {}".format(timedelta(seconds=end-start)))

