package com.vicodes.kafkaClient.core.kafka.serialization;

import com.vicodes.kafkaClient.core.kafka.dto.KafkaValueDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class ValueDtoDeserializer implements Deserializer<KafkaValueDto> {
  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {

  }

  @Override
  public KafkaValueDto deserialize(final String topic, final byte[] data) {
    ObjectMapper mapper = new ObjectMapper();
    KafkaValueDto valueDto = null;
    String val = new String(data);
    try {
      valueDto = mapper.readValue(data, KafkaValueDto.class);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return valueDto;
  }

  @Override
  public void close() {

  }
}
