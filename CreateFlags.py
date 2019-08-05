# Imports
import random
import requests
import cassandra 
from cassandra.cluster import Cluster 
from cassandra.auth import PlainTextAuthProvider

# Constants
ENVIRONMENTS = ['DEV', 'QA', 'PROD']
REQUEST_CATEGORIES = ['Activation',
    'Deactivation',
    'Stock Order',
    'Plan Change',
    'Profile Change',
    'State Change',
    'Reactivation',
    'MSISDN Swap',
    'SIM Change',
    'MSISDN Change',
    'Suspension',
    'Unsuspension',
    'Scrap2Stock',
    'Port Out Validation',
    'Port Out Cutover Confirmation',
    'Port Out Cutover Completion',
    'Port Out Withdrawal',
    'Bar Service',
    'Unbar Service']
SERVICE_TYPE_IDS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 15, 16, 19, 20, 21, 22, 23, 24, 25, 26, 27]

# Create flag strings
flags = []
for env in ENVIRONMENTS:
    for req_cat in REQUEST_CATEGORIES:
        for id in SERVICE_TYPE_IDS:
            flags.append([env, req_cat, id, 0, random.randint(0, 2)])
            flags.append([env, req_cat, id, 1, random.randint(0, 2)])

# Cassandra Connection
cass_address = 'dev-cassandra.ksg.int'
cass_port = 9042
cass_username = 'devadmin'
cass_password = 'Keys2TheK1ngd0m'
cass_namespace = 'CassandraPractice'
authentication = PlainTextAuthProvider(username=cass_username, password=cass_password)
cluster = Cluster([cass_address], port=cass_port, auth_provider=authentication)
session = cluster.connect(cass_namespace)

cass_query_prefix = "INSERT INTO \"Flags\" (\"Environment\", \"Request Category\", \"Service Type ID\", \"WFE\", \"Value\") VALUES ("
print(cass_query_prefix)

# Insert flags to cassandra table
for flag in flags:
    session.execute(cass_query_prefix + "'" + flag[0] + "'," + 
        "'" + flag[1] + "'," +
        str(flag[2]) + ',' +
        str(flag[3]) + ',' +
        str(flag[4]) + ');')
session.shutdown()

flipt_url = 'http://flipt-demo.devops-sandbox.k1d.k8.cin.kore.korewireless.com/api/v1/flags'

# Insert flags to flipt
for flag in flags:
    flag_name = "{}-{}-{}-{}".format(flag[0], flag[1], flag[2], flag[3]).lower()
    enabled = False
    if (flag[4]):
        enabled = True
    params = {"key":flag_name,"name":flag_name,"description":"zach POC key","enabled":enabled}
    response = requests.post(url=flipt_url, json=params)
    print(response)