package com.vicodes.kafkaClient.core.kafka.serialization;

import com.vicodes.kafkaClient.core.kafka.dto.KafkaValueDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ValueDtoSerializer implements Serializer<KafkaValueDto> {
  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {

  }

  @Override
  public byte[] serialize(final String topic, final KafkaValueDto data) {
    byte[] retVal = null;
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      retVal = objectMapper.writeValueAsString(data).getBytes();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return retVal;
  }

  @Override
  public void close() {

  }
}
