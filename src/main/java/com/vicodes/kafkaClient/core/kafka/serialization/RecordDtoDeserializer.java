package com.vicodes.kafkaClient.core.kafka.serialization;

import com.vicodes.kafkaClient.core.api.KafkaRecordDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class RecordDtoDeserializer implements Deserializer<KafkaRecordDto> {
  @Override
  public void configure(final Map configs, final boolean isKey) {

  }

  @Override
  public KafkaRecordDto deserialize(final String topic, final byte[] data) {
    ObjectMapper mapper = new ObjectMapper();
    KafkaRecordDto recordDto = null;
    String val = new String(data);
    try {
      recordDto = mapper.readValue(data, KafkaRecordDto.class);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return recordDto;
  }

  @Override
  public void close() {

  }
}
