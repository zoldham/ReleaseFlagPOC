# Imports
import redis
import requests
import threading
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
            flags.append([env, req_cat, id, 0])
            flags.append([env, req_cat, id, 1])

redis_done = False
cass_done = False
flipt_done = False
redis_result = None
cass_result = None
flipt_result = None

def get_flag_cass(flag, cass_instance):
    cass_select = "SELECT \"Value\" FROM \"Flags\" WHERE \"Environment\" = '{}' AND \"Request Category\" = '{}' AND \"Service Type ID\" = {} AND \"WFE\" = {};"
    results = session.execute(cass_select.format(flag[0], flag[1], flag[2], flag[3]))
    value = results[0][0]

    # Return results for both syncronous and asyncronous
    cass_result = value
    cass_done = True
    return value

def get_flag_redis(flag, redis_instance):
    redis_key = "{}-{}-{}-{}"
    value = redis_instance.get(redis_key.format(flag[0], flag[1], flag[2], flag[3]))

    flag = None
    if (value is None):
        flag = None
    elif (value.decode("utf-8") == "0"):
        flag = False
    else:
        flag = True

    # Return results for both syncronous and asyncronous
    redis_result = flag
    redis_done = True
    return flag

def get_flag_flipt(flag, flipt_url):
    flag_name = "{}-{}-{}-{}".format(flag[0], flag[1], flag[2], flag[3]).lower()
    query_url = flipt_url + flag_name
    response = requests.get(url = query_url)
    value = ('enabled' in response.json())

    # Return results for both syncronous and asyncronous
    flipt_result = value
    flipt_done = True
    return  value

def set_flag_redis(flag, value, redis_instance):
    redis_key = "{}-{}-{}-{}"
    value_str = "0"
    if (value):
        value_str = "1"
    redis_instance.set(redis_key.format(flag[0], flag[1], flag[2], flag[3]), value_str)

def get_flag_cass_redis_async(flag, cass_instance, redis_instance):
    redis_done = False
    cass_done = False
    redis_result = None
    cass_result = None
    return False

def get_flag_cass_redis_sync(flag, cass_instance, redis_instance):
    # Check redis instnace first
    value = get_flag_redis(flag, redis_instance)
    if (value is not None):
        return value

    # Now get it from cassandra and put it in redis
    value = get_flag_cass(flag, cass_instance)
    set_flag_redis(flag, value, redis_instance)
    return value

def get_flag_flipt_redis_async(flag, flipt_url, redis_instance):
    redis_done = False
    flipt_done = False
    redis_result = None
    flipt_result = None
    return False

def get_flag_flipt_redis_sync(flag, flipt_url, redis_instance):
    # Check redis instance first
    value = get_flag_redis(flag, redis_instance)
    if (value is not None):
        return value

    # Now get it from cassandra and put it in redis
    value = get_flag_flipt(flag, flipt_url)
    set_flag_redis(flag, value, redis_instance)
    return value

# Setup database connections
redis_remote_address = 'K1D-REDIS-CLST.ksg.int'
redis_remote_password = 'ZECjTH9cx24ukQA'
redis_local_address = 'localhost'
cass_address = 'dev-cassandra.ksg.int'
cass_port = 9042
cass_username = 'devadmin'
cass_password = 'Keys2TheK1ngd0m'
cass_namespace = 'CassandraPractice'
# Redis instnace
redis_remote = redis.Redis(host=redis_remote_address, password=redis_remote_password)
redis_local = redis.Redis(host=redis_local_address)
# Cassandra Connection
authentication = PlainTextAuthProvider(username=cass_username, password=cass_password)
cluster = Cluster([cass_address], port=cass_port, auth_provider=authentication)
session = cluster.connect(cass_namespace)
# Flipt Connection Info
flipt_url = 'http://flipt-demo.devops-sandbox.k1d.k8.cin.kore.korewireless.com/api/v1/flags/'

str1 = ""
str2 = ""
for flag in flags:
    if (get_flag_flipt_redis_sync(flag, flipt_url, redis_local)):
        str1 += '1'
    else:
        str1 += '0'

for flag in flags:
    if (get_flag_redis(flag, redis_local)):
        str2 += '1'
    else:
        str2 += '0'

print(str1 == str2)