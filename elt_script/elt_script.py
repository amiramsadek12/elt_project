import time
import subprocess


def wait_for_postgres(host, max_retries=5 , delay_seconds=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(["pg_isready", "-h", host], check=True, text=True, capture_output=True)

            if "accepting connections"  in result.stdout:
                print("connection established with success")
                return True  
        except subprocess.CalledProcessError as e:
            print(f"an error has occurred {e} ")
            retries += 1
            print(f"retrying in {delay_seconds}, retry: {retries}/{max_retries}" )
            time.sleep(delay_seconds)
    print("max retries reached... couldn't connect")
    return False

if not wait_for_postgres("source_postgres"):
    exit(1)

print("Starting ELT script...")

source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'
}

destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres'
}

dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'  
]


subprocess_env = dict(PGPASSWORD=source_config['password'])


subprocess.run(dump_command, env=subprocess_env, check=True)

load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

subprocess_env = dict(PGPASSWORD=destination_config['password'])

subprocess.run(load_command, env=subprocess_env, check=True)

print("Ending ELT script...")
