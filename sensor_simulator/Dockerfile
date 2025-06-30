FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY sensor_simulator.py .
COPY wait_for_kafka.py .

CMD ["python", "wait_for_kafka.py"]