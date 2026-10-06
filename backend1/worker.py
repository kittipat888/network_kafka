import json
import time
import paramiko
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "cisco-tasks"
CONSUMER_GROUP = "cisco-worker-group"

def execute_cisco(device_ip, username, password, command):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(
            hostname=device_ip,
            username=username,
            password=password,
            look_for_keys=False,
            allow_agent=False,
            timeout=10,
        )
        shell = ssh.invoke_shell()
        time.sleep(1)
        if shell.recv_ready():
            shell.recv(65535)

        shell.send("terminal length 0\n")
        time.sleep(0.5)

        shell.send(command + "\n")
        time.sleep(2)

        output = ""
        while shell.recv_ready():
            output += shell.recv(65535).decode("utf-8", errors="ignore")
        return output.strip()
    finally:
        ssh.close()

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"[*] Worker is listening on '{TOPIC_NAME}'...")
    for msg in consumer:
        event = msg.value
        event_id = event.get("event_id")
        created_at = event.get("created_at")
        cmd = event.get("command")
        ip = event.get("device_ip")

        print(f"\n==========================================")
        print(f"[EVENT RECEIVED]")
        print(f" Event ID   : {event_id}")
        print(f" Created At : {created_at}")
        print(f" Target IP  : {ip}")
        print(f" Command    : {cmd}")
        print(f"==========================================")

        try:
            result = execute_cisco(ip, event["username"], event["password"], cmd)
            print(f"[SUCCESS - {event_id}] Output:\n{result}\n")
        except Exception as e:
            print(f"[FAILED - {event_id}] Error: {e}\n")

if __name__ == "__main__":
    main()
