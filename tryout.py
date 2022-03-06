# import sqlalchemy
# from sshtunnel import SSHTunnelForwarder
#
# from configurations import connections_file_path
# import csv
# import urllib.parse
#
#
# def valid_connection(dictionary):
#     required = {'host', 'port', 'user', 'password'}
#     return required <= dictionary.keys()
#
#
# class Connection:
#
#     connect = 1
#     installation_id = 0
#     name = 'Default'
#     host = None
#     port = None
#     database = None
#     user = None
#     password = None
#     ssh_host = None
#     ssh_port = None
#     ssh_user = None
#     ssh_password = None
#     is_ssh = False
#
#     def __init__(self, dictionary):
#         self.__dict__.update(dictionary)
#         self.full_name = self.__repr__()
#         if self.ssh_host != '' and self.ssh_host is not None:
#             self.is_ssh = True
#             self.ssh_tunnel = None
#         self.engine = None
#
#     def __enter__(self):
#         return self.connect_to_src()
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         return self.disconnect_from_src()
#
#     def __repr__(self):
#         rep = '{}_{}'.format(self.name, self.installation_id)
#         return rep
#
#     def get_engine(self):
#         parsed_password = urllib.parse.quote_plus(self.password)
#         engine_string = "mysql+pymysql://{}:{}@{}:{}/{}" \
#             .format(self.user, parsed_password, self.host, str(self.port), self.database)
#         engine = sqlalchemy.create_engine(engine_string)
#         return engine
#
#     def connect_to_src(self):
#         if self.is_ssh:
#             ssh_tunnel = SSHTunnelForwarder(
#                 (self.ssh_host, int(self.ssh_port)),
#                 ssh_password=urllib.parse.quote_plus(self.ssh_password),
#                 ssh_username=self.ssh_user,
#                 remote_bind_address=(self.host, int(self.port)))
#             try:
#                 ssh_tunnel.start()
#                 self.ssh_tunnel = ssh_tunnel
#                 self.port = ssh_tunnel.local_bind_port
#                 self.engine = self.get_engine()
#                 print('Connected to {} over ssh'.format(self.name))
#                 return self.engine.begin()
#             except Exception as e:
#                 print('Connection over ssh failed: {}'.format(e))
#         else:
#             try:
#                 self.engine = self.get_engine()
#                 print('Connected to {}'.format(self.name))
#                 return self.engine.begin()
#             except Exception as e:
#                 print('Connection failed: {}'.format(e))
#                 return
#
#     def disconnect_from_src(self):
#         self.engine.dispose()
#         print('Disconnected from {}'.format(self.name))
#         if self.is_ssh:
#             self.ssh_tunnel.close()
#             print('SSH tunnel closed')
#
#
# file = connections_file_path
#
# with open(file, encoding="utf-8-sig") as f:
#     conns = [{k: v for k, v in row.items()}
#              for row in csv.DictReader(f, skipinitialspace=True)]
#
# connections = []
#
# for conn in conns:
#     if valid_connection(conn):
#         connections.append(Connection(conn))
#         print('Connection {} was added'.format(conn["name"]))
#     else:
#         print('Connection {} invalid'.format(conn["name"]))
#         continue
#
# print('hi')