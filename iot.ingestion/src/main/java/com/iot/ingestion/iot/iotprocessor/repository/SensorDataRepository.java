package com.iot.ingestion.iot.iotprocessor.repository;

import com.iot.ingestion.iot.iotprocessor.model.SensorData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SensorDataRepository extends JpaRepository<SensorData, Long> {

}
