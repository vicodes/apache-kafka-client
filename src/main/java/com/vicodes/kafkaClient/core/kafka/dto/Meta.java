package com.vicodes.kafkaClient.core.kafka.dto;

import lombok.Data;

@Data
public class Meta {

    private String topic;
    private Integer partition;
    private Long offset;
    private String correlationId;
    private String createdOn;
}
