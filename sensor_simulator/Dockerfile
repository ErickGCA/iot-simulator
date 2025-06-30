FROM python:3.10-slim
WORKDIR /app
COPY sensor_simulator.py .
RUN pip install confluent-kafka faker
CMD ["python", "sensor_simulator.py"]

