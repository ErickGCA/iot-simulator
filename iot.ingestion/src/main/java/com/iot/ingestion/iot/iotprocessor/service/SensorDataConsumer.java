package com.iot.ingestion.iot.iotprocessor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iot.ingestion.iot.iotprocessor.model.SensorData;
import com.iot.ingestion.iot.iotprocessor.repository.SensorDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class SensorDataConsumer {

    private final AtomicLong messageCounter = new AtomicLong(0);
    private static final Logger log = LoggerFactory.getLogger(SensorDataConsumer.class);
    private final ObjectMapper objectMapper;
    private final SensorDataRepository sensorDataRepository;

    public SensorDataConsumer(ObjectMapper objectMapper, SensorDataRepository sensorDataRepository) {
        this.objectMapper = objectMapper;
        this.sensorDataRepository = sensorDataRepository;
    }

    @KafkaListener(topics = "sensor_data_raw", groupId = "iot_processor_group_v3")
    @Transactional
    public void consume(String message) {
        long count = messageCounter.incrementAndGet(); // Incrementa o contador
        try {
            JsonNode root = objectMapper.readTree(message);

            SensorData data = new SensorData();
            data.setSensorId(root.path("sensorId").asText());
            data.setLocationLatitude(root.path("location").path("latitude").asText());
            data.setLocationLongitude(root.path("location").path("longitude").asText());
            data.setType(root.path("type").asText());
            data.setValue(root.path("value").asDouble());
            data.setTimestamp(root.path("timestamp").asLong());
            data.setProcessedByThread(Thread.currentThread().toString());

            sensorDataRepository.save(data);

            if (count % 10000 == 0) {
                log.info("Processadas {} mensagens. Última salva: sensorId={}, Thread: {}",
                        count, data.getSensorId(), data.getProcessedByThread());
            }

        } catch (Exception e) {
            log.error("Erro ao processar mensagem do Kafka: {}", message, e);
        }
    }
}