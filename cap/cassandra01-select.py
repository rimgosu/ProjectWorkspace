from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
import json

file_path = r'C:\keys\keyspace\application.json'

with open(file_path, 'r', encoding='utf-8') as file:
    data = json.load(file)

USERNAME = data["username"]
PASSWORD = data["password"]

ssl_context = SSLContext(PROTOCOL_TLSv1_2 )
ssl_context.load_verify_locations('c:/keys/keyspace/sf-class2-root.crt')
ssl_context.verify_mode = CERT_REQUIRED
auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
cluster = Cluster(['cassandra.ap-northeast-2.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142)
session = cluster.connect()
r = session.execute('select * from member.info')
print(r.current_rows)