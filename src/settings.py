import os
import json

def get_compose_inspect():
    if os.path.exists('compose_inspect.json'):
        with open('compose_inspect.json', 'r') as f:
            return json.load(f)
    else:
        return {}

def get_env(env, var_name):
    items = [v.split('=')[1] for v in env if v.startswith(var_name+'=')]
    if items:
        return items[0]

inspect = get_compose_inspect()

PG_HOST = os.environ.get("PG_HOST", inspect["postgres_db_lab"]["network"]["spark_app_network"]["ip"] if inspect else None)
PG_PORT = os.environ.get("PG_PORT", inspect["postgres_db_lab"]["ports"][0].split('/')[0] if inspect else None)
PG_USER = os.environ.get("PG_USER", get_env(inspect["postgres_db_lab"]["env"], "POSTGRES_USER") if inspect else None)
PG_PASSWORD = os.environ.get("PG_PASSWORD", get_env(inspect["postgres_db_lab"]["env"],"POSTGRES_PASSWORD") if inspect else None)
PG_DB = os.environ.get("PG_DB", get_env(inspect["postgres_db_lab"]["env"],"POSTGRES_DB") if inspect else None)

SPARK_HOST = os.environ.get("SPARK_HOST", inspect["spark_master"]["network"]["spark_app_network"]["ip"] if inspect else None)
SPARK_PORT = os.environ.get("SPARK_PORT", 7077)
SPARK_DRIVER_IP = os.environ.get("SPARK_DRIVER_IP", inspect["spark_master"]["network"]["spark_app_network"]["gateway"] if inspect else None)

