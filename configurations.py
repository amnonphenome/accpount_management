from pathlib import Path

# dest db_account_management
dest_config = {
    "host": "localhost",
    "port": 3306,
    "database": "db_account_management",
    "user": "root",
    "password": "tnbui115"}

qa_config = {
    "host": "52.16.7.222",
    "port": 3306,
    "database": "pheno20",
    "user": "phenome",
    "password": "fCK@MiV^84wP"}

unity_config = {
    "host": "52.16.89.59",
    "port": 3306,
    "database": "pheno20",
    "user": "phenome",
    "password": "9azaDRu5"}

# connections file
files_path = Path("C:\\Users\\amnon\\Documents\\Python_Scripts\\mysql_connection\\")
connections_file_path = Path(files_path, "connections.csv")

# queries JSON
queries = {
    "users_actions": {
        "name": "users_actions",
        "query_string": "SELECT * FROM users_actions"},
    "users_actions_categories": {
        "name": "users_actions_categories",
        "query_string": "SELECT * FROM users_actions_categories"},
    "application_modules": {
        "name": "application_modules",
        "query_string": "SELECT * FROM application_modules"},
    "users_actions_types": {
        "name": "users_actions_types",
        "query_string": "SELECT * FROM users_actions_types"},
    "installations": {
        "name": "installations",
        "query_string": "SELECT * FROM installations"},
    "orgs": {
        "name": "orgs",
        "query_string": "SELECT o.id, o.name organization, c.name country FROM organizations o join countries c on "
                        "o.country_id=c.id"},
    "users": {
        "name": "users",
        "query_string": "SELECT organization_id, id user_id, name username, role user_role, language_id language, "
        "creation_date, is_adminisrator admin, active, create_research_group create_RG, varieties FROM users"},
    "users_actions_log": {
        "name": "users_actions_log",
        "query_string": "SELECT id, uuid, user_id, date, action_id, research_group_id, folder_id, object_id, "
                        "object_name, installation_id FROM users_actions_log where id > "},  # input max_id
}
