package com.vicodes.kafkaClient.core.kafka.serialization;

import com.vicodes.kafkaClient.core.api.KafkaRecordDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class RecordDtoSerializer implements Serializer<KafkaRecordDto> {
  @Override
  public void configure(final Map configs, final boolean isKey) {

  }

  @Override
  public byte[] serialize(final String topic, final KafkaRecordDto data) {
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
