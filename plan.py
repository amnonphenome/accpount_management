# get_connections
# define queries dict

# db_account_management structure
# Fixed tables in db_account_management
#   "users_actions"
#   "users_actions_categories"
#   "application_modules"
#   "users_actions_types"
#   "installations"
# combined tables (Small) in db_account_management >> am
#   am_users
#   am_organizations
# Combined tables (Large) in db_account_management >> am
#   am_ual (combined users_actions_log)
#   am_ufo (combined unified_field_observations)
# View  : max_ual group by installation_id
# View  : rg_stats group by ufo.rg_id
# View? : users_stats

# db initiate (at first and at some version updates)
#   define src = qa
#       create_engine_string(**kwargs)
#   define dest = db_account_management
#       create_engine_string(**kwargs)
#   fixed_tables_to_update = ["users_actions", "users_actions_categories", "application_modules", "users_actions_types",
#                               "installations"]
#   update_fixed_tables(src_engine, dest_engine, tables_list)

# get max_ual from db_account_management
#   with connect dest
#   df_ual_max_per_env = read_sql view holding max ual per env
#       SELECT installation_id, max_ual FROM db_account_management.max_ual

# Get data from connections:
# define df_data(orgs, users, ual)
# for conn in connections
#   define src = conn
#       create_engine_string(**kwargs)
#   with src_engine
#       read_sql organizations to df[organizations]
#       read_sql users to df[users]
#       define conn.max_ual
#       read_sql_query (where ual.id > conn.ual_max) to df[ual]
#   with dest_engine
#       to_sql organizations ?ifexist='ignore'
#       primary key organizations
#       to_sql users ?ifexist='ignore'
#       primary key users
#       to_sql ual
#       primary key ual?
#   clear df

