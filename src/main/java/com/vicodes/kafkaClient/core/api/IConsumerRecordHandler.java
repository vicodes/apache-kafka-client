package com.vicodes.kafkaClient.core.api;

import com.vicodes.kafkaClient.core.kafka.dto.KafkaValueDto;

public interface IConsumerRecordHandler {
    boolean execute(KafkaValueDto var1) throws Exception;
}