# Imports
import redis
import random
import requests
import threading
import cassandra 
import numpy as np
from cassandra.cluster import Cluster 
from cassandra.auth import PlainTextAuthProvider
import time

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

# Global variables for async ops
MAX_TREADS = 1000
num_threads = 0
num_threads_mutex = threading.Lock()
redis_done = False
cass_done = False
flipt_done = False
redis_result = None
cass_result = None
flipt_result = None
cycle_num = 0
cycle_num_mutex = threading.Lock()

# Remove given flags from redis cache
def remove_flags_redis(flags, redis_instance):
    redis_key = "{}-{}-{}-{}"
    for flag in flags:
        redis_instance.delete(redis_key.format(flag[0], flag[1], flag[2], flag[3]))

# Add given percent of given flags to redis cache
def add_partial_flags_redis(flags, values, rate, redis_instance):
    for flag, value in zip(flags, values):
        if (random.uniform(0, 1) < rate):
            set_flag_redis(flag, value, redis_instance)

# Get the given flag from cassandra
def get_flag_cass(flag, cass_instance):

    # 'Declarations'
    global cass_result 
    global cass_done

    cass_select = "SELECT \"Value\" FROM \"Flags\" WHERE \"Environment\" = '{}' AND \"Request Category\" = '{}' AND \"Service Type ID\" = {} AND \"WFE\" = {};"
    results = cass_instance.execute(cass_select.format(flag[0], flag[1], flag[2], flag[3]))
    value = results[0][0]

    return value

# Attempt to get the given flag from redis
def get_flag_redis(flag, redis_instance):

    # 'Declarations'
    global redis_result
    global redis_done

    redis_key = "{}-{}-{}-{}"
    value = redis_instance.get(redis_key.format(flag[0], flag[1], flag[2], flag[3]))

    flag = None
    if (value is None):
        flag = None
    elif (value.decode("utf-8") == "0"):
        flag = False
    else:
        flag = True

    return flag

# Get the given flag from flipt
def get_flag_flipt(flag, flipt_url):

    # 'Declarations'
    global flipt_result
    global flipt_done

    flag_name = "{}-{}-{}-{}".format(flag[0], flag[1], flag[2], flag[3]).lower()
    query_url = flipt_url + flag_name
    response = requests.get(url = query_url)
    value = ('enabled' in response.json())

    return  value

# Determine if we should start an async op, or wait
def start_async():

    # 'Declarations'
    global num_threads
    global num_threads_mutex
    global MAX_TREADS
    result = False

    # Thread-safe check
    num_threads_mutex.acquire()
    if (num_threads >= MAX_TREADS - 2):
        result = False
    else:
        result = True  
    num_threads_mutex.release()

    return result

# Retrieve the flag from cassandra. 
def get_flag_cass_threaded(flag, cass_instance, start_cycle):

    # 'Declarations'
    global cass_done 
    global cass_result
    global num_threads
    global num_threads_mutex
    global cycle_num
    global cycle_num_mutex

    num_threads_mutex.acquire()
    num_threads = num_threads + 1
    num_threads_mutex.release()
    try:
        cass_select = "SELECT \"Value\" FROM \"Flags\" WHERE \"Environment\" = '{}' AND \"Request Category\" = '{}' AND \"Service Type ID\" = {} AND \"WFE\" = {};"
        results = cass_instance.execute(cass_select.format(flag[0], flag[1], flag[2], flag[3]))
        value = results[0][0]

        # Return results 
        cycle_num_mutex.acquire()
        if (cycle_num == start_cycle):
            cass_result = value
            cass_done = True
        cycle_num_mutex.release()
    finally:
        num_threads_mutex.acquire()
        num_threads = num_threads - 1
        num_threads_mutex.release()

# Retrieve the flag from redis if possible
def get_flag_redis_threaded(flag, redis_instance, start_cycle):

    # 'Declarations'
    global redis_done 
    global redis_result
    global num_threads
    global num_threads_mutex
    global cycle_num
    global cycle_num_mutex

    num_threads_mutex.acquire()
    num_threads = num_threads + 1
    num_threads_mutex.release()
    try:
        redis_key = "{}-{}-{}-{}"
        value = redis_instance.get(redis_key.format(flag[0], flag[1], flag[2], flag[3]))

        result = None
        if (value is None):
            result = None
        elif (value.decode("utf-8") == "0"):
            result = False
        else:
            result = True

        # Return results 
        cycle_num_mutex.acquire()
        if (cycle_num == start_cycle):
            redis_result = result
            redis_done = True
        cycle_num_mutex.release()
    finally:
        num_threads_mutex.acquire()
        num_threads = num_threads - 1
        num_threads_mutex.release()

# Retrieve the flag from flipt
def get_flag_flipt_threaded(flag, flipt_url, start_cycle):

    # 'Declarations'
    global flipt_done 
    global flipt_result
    global num_threads
    global num_threads_mutex
    global cycle_num
    global cycle_num_mutex

    num_threads_mutex.acquire()
    num_threads = num_threads + 1
    num_threads_mutex.release()
    try:
        flag_name = "{}-{}-{}-{}".format(flag[0], flag[1], flag[2], flag[3]).lower()
        query_url = flipt_url + flag_name
        response = requests.get(url = query_url)
        value = ('enabled' in response.json())

        # Return results 
        cycle_num_mutex.acquire()
        if (cycle_num == start_cycle):
            flipt_result = value
            flipt_done = True
        cycle_num_mutex.release()
    finally:
        num_threads_mutex.acquire()
        num_threads = num_threads - 1
        num_threads_mutex.release()

# Create the key-value pair flag-value in the specified redis instance
def set_flag_redis(flag, value, redis_instance):
    redis_key = "{}-{}-{}-{}"
    value_str = "0"
    if (value):
        value_str = "1"
    redis_instance.set(redis_key.format(flag[0], flag[1], flag[2], flag[3]), value_str)

# Get flag from redis if possible, cassandra if not. Requests made asyncronously. NOT THREAD SAFE.
def get_flag_cass_redis_async(flag, cass_instance, redis_instance):

    # Wait for number of threads to become low enough to start
    while (not start_async()):
        time.sleep(0.001)
    
    # 'Declarations'
    global redis_done
    global cass_done
    global redis_result
    global cass_result
    global cycle_num
    global cycle_num_mutex
    value = None
    
    # Reset async global vars
    redis_done = False
    cass_done = False
    redis_result = None
    cass_result = None

    # Start threads
    cycle_num_mutex.acquire()
    redis_thread = threading.Thread(target=get_flag_redis_threaded, args=(flag, redis_instance, cycle_num))
    cass_thread = threading.Thread(target=get_flag_cass_threaded, args=(flag, cass_instance, cycle_num))
    cycle_num_mutex.release()
    redis_thread.setDaemon(True)
    cass_thread.setDaemon(True)
    redis_thread.start()
    cass_thread.start()
    while (True):

        # Check redis thread
        if (redis_done):
            redis_thread.join()
            value = redis_result
            if (value is None):
                redis_done = False
                value = None
            else:
                #print('Redis')
                break

        # Check cass thread
        if (cass_done):
            cass_thread.join()
            value = cass_result
            set_flag_redis(flag, value, redis_instance)
            #print('Cass')
            break

        time.sleep(0.001)

    cycle_num_mutex.acquire()
    cycle_num = cycle_num + 1
    cycle_num_mutex.release()
    return value

# Get flag from redis if possible, cassandra if not. Requests made syncronously.
def get_flag_cass_redis_sync(flag, cass_instance, redis_instance):
    # Check redis instnace first
    value = get_flag_redis(flag, redis_instance)
    if (value is not None):
        return value

    # Now get it from cassandra and put it in redis
    value = get_flag_cass(flag, cass_instance)
    set_flag_redis(flag, value, redis_instance)
    return value

# Get flag from redis if possible, flipt if not. Requests made asyncronously NOT THREAD SAFE.
def get_flag_flipt_redis_async(flag, flipt_url, redis_instance):

    # Wait for number of threads to become low enough to start
    while (not start_async()):
        time.sleep(0.001)
    
    # 'Declarations'
    global redis_done
    global flipt_done
    global redis_result
    global flipt_result
    global cycle_num
    global cycle_num_mutex
    value = None
    
    # Reset async global vars
    flipt_done = False
    redis_done = False
    flipt_result = None
    redis_result = None

    # Start threads
    cycle_num_mutex.acquire()
    redis_thread = threading.Thread(target=get_flag_redis_threaded, args=(flag, redis_instance, cycle_num))
    flipt_thread = threading.Thread(target=get_flag_flipt_threaded, args=(flag, flipt_url, cycle_num))
    cycle_num_mutex.release()
    redis_thread.setDaemon(True)
    flipt_thread.setDaemon(True)
    redis_thread.start()
    flipt_thread.start()
    while (True):

        # Check redis thread
        if (redis_done):
            redis_thread.join()
            value = redis_result
            if (value is None):
                redis_done = False
                value = None
            else:
                #print('Redis')
                break

        # Check flipt thread
        if (flipt_done):
            flipt_thread.join()
            value = flipt_result
            set_flag_redis(flag, value, redis_instance)
            #print('Flipt')
            break
        
        time.sleep(0.001)

    cycle_num_mutex.acquire()
    cycle_num = cycle_num + 1
    cycle_num_mutex.release()
    return value

# Get flag from redis if possible, flipt if not. Requests made syncronously.
def get_flag_flipt_redis_sync(flag, flipt_url, redis_instance):
    # Check redis instance first
    value = get_flag_redis(flag, redis_instance)
    if (value is not None):
        return value

    # Now get it from cassandra and put it in redis
    value = get_flag_flipt(flag, flipt_url)
    set_flag_redis(flag, value, redis_instance)
    return value

# Get the function 'names' for the corresponding which in multicaller
def multinamer(which):
    if (which == 0):    # Cassandra
        return "Cassandra"
    elif (which == 1):  # Cassandra - redis - sync - local
        return "Cassandra - redis - sync - local"
    elif (which == 2):  # Cassandra - redis - sync - remote
        return "Cassandra - redis - sync - remote"
    elif (which == 3):  # Cassandra - redis - async - local
        return "Cassandra - redis - async - local"
    elif (which == 4):  # Cassandra - redis - async - remote
        return "Cassandra - redis - async - remote"
    elif (which == 5):  # Flipt
        return "Flipt"
    elif (which == 6):  # Flipt - redis - sync - local
        return "Flipt - redis - sync - local"
    elif (which == 7):  # Flipt - redis - sync - remote
        return "Flipt - redis - sync - remote"
    elif (which == 8):  # Flipt - redis - async - local
        return "Flipt - redis - async - local"
    elif (which == 9):  # Flipt - redis - async - remote
        return "Flipt - redis - async - remote"

# Multicaller - for cleaner code
def multicaller(flag, cass_instance, flipt_url, redis_local, redis_remote, which):
    if (which == 0):    # Cassandra
        return get_flag_cass(flag, cass_instance)
    elif (which == 1):  # Cassandra - redis - sync - local
        return get_flag_cass_redis_sync(flag, cass_instance, redis_local)
    elif (which == 2):  # Cassandra - redis - sync - remote
        return get_flag_cass_redis_sync(flag, cass_instance, redis_remote)
    elif (which == 3):  # Cassandra - redis - async - local
        return get_flag_cass_redis_async(flag, cass_instance, redis_local)
    elif (which == 4):  # Cassandra - redis - async - remote
        return get_flag_cass_redis_async(flag, cass_instance, redis_remote)
    elif (which == 5):  # Flipt
        return get_flag_flipt(flag, flipt_url)
    elif (which == 6):  # Flipt - redis - sync - local
        return get_flag_flipt_redis_sync(flag, flipt_url, redis_local)
    elif (which == 7):  # Flipt - redis - sync - remote
        return get_flag_flipt_redis_sync(flag, flipt_url, redis_remote)
    elif (which == 8):  # Flipt - redis - async - local
        return get_flag_flipt_redis_async(flag, flipt_url, redis_local)
    elif (which == 9):  # Flipt - redis - async - remote
        return get_flag_flipt_redis_async(flag, flipt_url, redis_remote)

# Database config and setup
redis_remote_address = 'K1D-REDIS-CLST.ksg.int'
redis_remote_password = 'ZECjTH9cx24ukQA'
redis_local_address = 'localhost'
cass_address = 'dev-cassandra.ksg.int'
cass_port = 9042
cass_username = 'devadmin'
cass_password = 'Keys2TheK1ngd0m'
cass_namespace = 'CassandraPractice'
# Redis instance
redis_remote = redis.Redis(host=redis_remote_address, password=redis_remote_password)
redis_local = redis.Redis(host=redis_local_address)
# Cassandra Connection
authentication = PlainTextAuthProvider(username=cass_username, password=cass_password)
cluster = Cluster([cass_address], port=cass_port, auth_provider=authentication)
cass_session = cluster.connect(cass_namespace)
# Flipt Connection Info
flipt_url = 'http://flipt-demo.devops-sandbox.k1d.k8.cin.kore.korewireless.com/api/v1/flags/'

# Get the values for storage
values = []
for flag in flags:
    values.append(get_flag_cass(flag, cass_session))

# Test config
num_redis_percents = 11
num_repetitions = 7
output_file = 'output.csv'

data = np.zeros((num_redis_percents, 10))

# Iterate over various % in redis cache
i = -1
for redis_percent in np.linspace(0, 1, num_redis_percents):
    i = i + 1

    # Iterate over which method/config is used
    for which in range(0, 10):

        # Setup aggregation
        total_time = 0.0
    
        # Iterate set number of times
        for i in range(0, num_repetitions):
            
            # Setup redis
            remove_flags_redis(flags, redis_local)
            remove_flags_redis(flags, redis_remote)
            add_partial_flags_redis(flags, values, redis_percent, redis_local)
            add_partial_flags_redis(flags, values, redis_percent, redis_remote)

            # Wait for threads
            while (num_threads > 0):
                time.sleep(0.01)

            # Time the retrievals
            start_time = time.time()
            for flag in flags:
                multicaller(flag, cass_session, flipt_url, redis_local, redis_remote, which)
            total_time = total_time + time.time() - start_time

        # Done with this method-percent combo
        avg_time = total_time / num_repetitions
        print("{} - {}{} hit rate: {} seconds".format(multinamer(which), redis_percent * 100, "%", avg_time))
        data[i][which]

# Format output
data = np.transpose(data)
output = []

# Header
header = "Redis Hit Rate: "
for percent in np.linspace(0, 1, num_redis_percents):
    header = header + ',' + str(percent)
header = header + '\n'
output.append(header)

# Data rows
for which in range(0, 10):
    line = multinamer(which)
    for i in range(0, num_redis_percents):
        line = line + ',' + str(data[which][i])
    line = line + '\n'
    output.append(line)

# Output to file
with open(output_file, "w") as file:
    for line in output:
        file.write(line)
