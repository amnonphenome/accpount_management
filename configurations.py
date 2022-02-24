from pathlib import Path

# dest db_account_management
dest_config = {
    "host": "localhost",
    "port": 3306,
    "database": "db_account_management",
    "user": "root",
    "password": "tnbui115"}

qa_config = {
    "name": "qa",
    "host": "52.16.7.222",
    "port": 3306,
    "database": "pheno20",
    "user": "phenome",
    "password": "fCK@MiV^84wP",
    "ssh_host": None}  # needed for ruling out ssh connection

unity_config = {
    "name": "unity",
    "host": "52.16.89.59",
    "port": 3306,
    "database": "pheno20",
    "user": "phenome",
    "password": "9azaDRu5",
    "ssh_host": None}  # needed for ruling out ssh connection

# connections file
files_path = Path("C:\\Users\\amnon\\Documents\\Python_Scripts\\mysql_connection\\")
connections_file_path = Path(files_path, "connections.csv")

# queries JSON
queries = {
    "am_users_actions": {
        "name": "am_users_actions",
        "query_string": "SELECT ua.id, ua.name, ua.path, ua.arguments, ua.action_type_id, uat.name action_type, "
                        "ua.category_id, uac.name category, ua.appmod_id, appmod.name appmod FROM users_actions ua "
                        "LEFT JOIN users_actions_categories uac ON ua.category_id = uac.id LEFT JOIN "
                        "application_modules appmod ON appmod.id = ua.appmod_id LEFT JOIN users_actions_types uat ON "
                        "uat.id = ua.action_type_id"},
    "installations": {
        "name": "installations",
        "query_string": "SELECT * FROM installations"},
    "orgs": {
        "name": "orgs",
        "query_string": "SELECT o.id organization_id, o.name organization, c.name country FROM organizations o join "
                        "countries c on "
                        "o.country_id=c.id"},
    "users": {
        "name": "users",
        "query_string": "SELECT organization_id, id user_id, name username, role user_role, language_id language, "
        "creation_date, is_adminisrator admin, active, create_research_group create_RG, varieties FROM users"},
    "users_actions_log": {
        "name": "users_actions_log",
        "query_string": "SELECT id, user_id, date, action_id FROM pheno20.users_actions_log where id > {} "}  # max_id
}
