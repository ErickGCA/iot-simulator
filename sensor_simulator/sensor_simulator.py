from confluent_kafka import Producer
from faker import Faker
import json
import time
import random
import sys
import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = 'sensor_data_raw'
NUM_MESSAGES_TO_PRODUCE = 1_000_000 
BATCH_SIZE = 5_000 

producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(producer_conf)

fake = Faker('pt_BR')

def generate_sensor_data(sensor_id_base):
    """Gera dados de um sensor simulado."""
    sensor_id = f"sensor_{sensor_id_base}_{random.randint(100, 999)}"
    location = {
        "latitude": float(fake.latitude()),
        "longitude": float(fake.longitude())
    }

    sensor_type = random.choice([
        "umidade_solo", "temperatura_ar", "luminosidade", "ph_solo", "nivel_nutrientes"
    ])

    value = 0.0
    if sensor_type == "umidade_solo":
        value = round(random.uniform(20.0, 80.0), 2) # %
    elif sensor_type == "temperatura_ar":
        value = round(random.uniform(15.0, 40.0), 2) # Celsius
    elif sensor_type == "luminosidade":
        value = round(random.uniform(1000.0, 100000.0), 2) # Lux
    elif sensor_type == "ph_solo":
        value = round(random.uniform(5.0, 8.0), 1) # pH
    elif sensor_type == "nivel_nutrientes":
        value = round(random.uniform(0.1, 5.0), 2) # PPM ou similar

    timestamp = int(time.time() * 1000) # Epoch em milissegundos

    return {
        "sensorId": sensor_id,
        "location": location,
        "type": sensor_type,
        "value": value,
        "timestamp": timestamp
    }

def delivery_report(err, msg):
    """Callback chamado após a entrega da mensagem (opcional, para depuração)."""
    if err is not None:
        sys.stderr.write(f'Falha na entrega da mensagem para {msg.topic()}: {err}\n')
    # else:
        # print(f"Mensagem entregue para {msg.topic()} [{msg.partition()}]")

def produce_messages():
    """Produz um número especificado de mensagens para o Kafka."""
    print(f"Iniciando a produção de {NUM_MESSAGES_TO_PRODUCE} mensagens para o tópico '{KAFKA_TOPIC}'...")
    start_time = time.time()

    for i in range(NUM_MESSAGES_TO_PRODUCE):
        sensor_id_base = i % 2000 
        data = generate_sensor_data(sensor_id_base)

        producer.produce(
            KAFKA_TOPIC, 
            key=str(data["sensorId"]).encode('utf-8'), 
            value=json.dumps(data).encode('utf-8'), 
            callback=delivery_report
        )

        if i % BATCH_SIZE == 0:
            producer.poll(0) 
            sys.stdout.write(f'\rProduzindo... {i} mensagens enviadas.')
            sys.stdout.flush()

    print("\nFinalizando produção e aguardando confirmação de entrega de todas as mensagens...")
    producer.flush() 
    end_time = time.time()
    print(f"Produção de {NUM_MESSAGES_TO_PRODUCE} mensagens finalizada em {end_time - start_time:.2f} segundos.")
    print(f"Verifique o console do serviço Java para ver o consumo.")

if __name__ == "__main__":
    try:
        import confluent_kafka
        import faker
    except ImportError:
        print("Instalando dependências Python (confluent-kafka, faker)...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "confluent-kafka", "faker"])
        print("Dependências instaladas. Por favor, rode o script novamente.")
        sys.exit(1)

    produce_messages()