import uuid
from datetime import datetime, timezone
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from kafka import KafkaProducer

app = FastAPI(title="Cisco Automation API")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC_NAME = "cisco-tasks"

# Request ที่รับจาก Client/User
class CommandRequest(BaseModel):
    device_ip: str = "192.168.149.134"
    username: str = "admin"
    password: str = "cisco"
    command: str = "show ip interface brief"

@app.post("/execute")
def send_command(req: CommandRequest):
    try:
        # สร้าง Event Envelope ตามมาตรฐาน
        event_payload = {
            "event_id": str(uuid.uuid4()),                    # รหัส Event ID แบบไม่ซ้ำ
            "event_type": "CISCO_COMMAND_EXECUTE",
            "created_at": datetime.now(timezone.utc).isoformat(), # Timestamp เวลาที่สร้าง
            "device_ip": req.device_ip,
            "username": req.username,
            "password": req.password,
            "command": req.command,
        }

        # ส่งเข้า Kafka Topic
        future = producer.send(TOPIC_NAME, value=event_payload)
        metadata = future.get(timeout=10)

        return {
            "message": "Task queued successfully",
            "event_id": event_payload["event_id"],
            "created_at": event_payload["created_at"],
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
