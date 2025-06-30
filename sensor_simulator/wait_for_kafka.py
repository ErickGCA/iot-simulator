import sys
import time
import os
import subprocess
from confluent_kafka.admin import AdminClient

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
print(f"Tentando conectar ao Kafka em: {KAFKA_BOOTSTRAP_SERVERS}")

conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
admin_client = AdminClient(conf)

while True:
    try:
        topics = admin_client.list_topics(timeout=5.0).topics
        print(">>> Kafka está PRONTO! Tópicos encontrados:", list(topics.keys()))
        break 
    except Exception as e:
        print(f"Aguardando o Kafka ficar pronto (Erro: {e})... Tentando novamente em 5 segundos.")
        time.sleep(5)

print("\n>>> Iniciando o simulador de sensores...")

try:
    subprocess.run(["python", "sensor_simulator.py"], check=True)
except subprocess.CalledProcessError as e:
    print(f"O script sensor_simulator.py falhou com código de saída {e.returncode}")
    sys.exit(e.returncode)
except KeyboardInterrupt:
    print("Simulação interrompida pelo usuário.")
    sys.exit(0)