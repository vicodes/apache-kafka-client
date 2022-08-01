package com.vicodes.kafkaClient.core.api;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotNull;
import java.util.Map;

@Getter
@Setter
@Builder
public class ConsumerScheduledTaskConfig {

  @NotNull
  private String taskName;

  @NotNull
  private String topic;

  @NotNull
  private String consumerGroup;

  @NotNull
  private long periodOfConsumptionInMillis;

  private String maxPollRecords;

  private int instanceCount = 1;

  @NotNull
  private boolean active;

  private Map<String, Object> properties;
  private String maxBytes;
  private Integer maxRetryCount;

}
