package com.iot.ingestion.iot.iotprocessor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iot.ingestion.iot.iotprocessor.model.SensorData;
import com.iot.ingestion.iot.iotprocessor.repository.SensorDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SensorDataConsumer {

    private static final Logger log = LoggerFactory.getLogger(SensorDataConsumer.class);
    private final ObjectMapper objectMapper;
    private final SensorDataRepository sensorDataRepository;

    public SensorDataConsumer(ObjectMapper objectMapper, SensorDataRepository sensorDataRepository) {
        this.objectMapper = objectMapper;
        this.sensorDataRepository = sensorDataRepository;
    }

    public void consume(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);

            SensorData data = new SensorData();
            data.setSensorId(root.path("sensorId").asText());
            data.setLocationLatitude(root.path("location").path("latitude").asText());
            data.setLocationLongitude(root.path("location").path("longitude").asText());
            data.setType(root.path("type").asText());
            data.setValue(root.path("value").asDouble());
            data.setTimestamp(root.path("timestamp").asLong());
            data.setProcessedByThread(Thread.currentThread().getName());

            sensorDataRepository.save(data);
            log.info("Mensagem processada e salva com sucesso: sensorId={}", data.getSensorId());
        } catch (Exception e) {
            log.error("Erro ao processar mensagem do Kafka: {}", message, e);
        }
    }
}
