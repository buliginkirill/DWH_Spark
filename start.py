#!./.venv/bin/python

import subprocess
import sys
import argparse
import json
from yaml import safe_load

def start_docker_compose():
    print("Starting Docker Compose...")
    result = subprocess.run(["docker", "compose", "-f", "compose.yaml", "up", "-d"], capture_output=True, text=True)
    if result.returncode == 0:
        print("Docker Compose started.")
    else:
        print(f"Error starting Docker Compose: {result.stderr}")

def stop_docker_compose():
    print("Stopping Docker Compose...")
    result = subprocess.run(["docker", "compose", "-f", "compose.yaml", "down"], capture_output=True, text=True)
    if result.returncode == 0:
        print("Docker Compose stopped.")
    else:
        print(f"Error stopping Docker Compose: {result.stderr}")

def usage():
    print("Usage: python manage_docker_compose.py {start|stop}")
    sys.exit(1)


def inspect_containers(filename = None):
    with open("compose.yaml", "r") as f:
        compose = safe_load(f)
    services = [v['container_name'] for k,v in compose['services'].items()]
    result = {}
    for s in services:
        print(f"Inspecting {s}...")
        result = {**result, **inspect(s)}
    if filename:
        with open(filename, "w") as f:
            json.dump(result, f, indent=4, sort_keys=True)
    print(json.dumps(result, indent=4, sort_keys=True))


def inspect(container_name):
    def extract_network_data(networks):
        res = {}
        for k,v in networks.items():
            res[k] = {
                "ip": v['IPAddress'],
                "gateway": v['Gateway'],
                "dns_names": v['DNSNames']
            }
        return res


    print("Inspecting Docker containers...")
    result = subprocess.run(["docker", "inspect", container_name], capture_output=True, text=True)
    try:
        result_json = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}
    if result.returncode == 0:
        try:
            #print(json.dumps(result_json, indent=4, sort_keys=True))
            return {result_json[0]['Name'].strip('/'): {
              "running": result_json[0]['State']['Running'],
              "ports": list(result_json[0]['NetworkSettings']['Ports'].keys()),
              "env": result_json[0]['Config']['Env'],
              "network": extract_network_data(result_json[0]['NetworkSettings']['Networks']),
            }}
        except IndexError:
            return {}
    return {}


def main():
    parser = argparse.ArgumentParser(description="Manage Docker Compose services.")
    parser.add_argument("action", choices=["start", "stop", "inspect"], help="Action to perform: start or stop")
    args = parser.parse_args()

    if args.action == "start":
        start_docker_compose()
        inspect_containers('compose_inspect.json')
    elif args.action == "stop":
        stop_docker_compose()
    elif args.action == "inspect":
        inspect_containers('compose_inspect.json')
    else:
        usage()

if __name__ == "__main__":
    main()