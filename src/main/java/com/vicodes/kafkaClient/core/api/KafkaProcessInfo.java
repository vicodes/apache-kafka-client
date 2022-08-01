package com.vicodes.kafkaClient.core.api;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
public class KafkaProcessInfo {
    private String consumerGroup;
    private String topic;
    private boolean consumedSuccessfully;
    private int partition;
    private long offset;
    private long processingTime;

    @Override
    public String toString() {
        return "consumed on kafka with: [" +
                "consumerGroup='" + consumerGroup + '\'' +
                ", topic='" + topic + '\'' +
                ", consumedSuccessfully=" + consumedSuccessfully +
                ", partition=" + partition +
                ", offset=" + offset +
                ", processingTime=" + processingTime +
                ']';
    }
}
