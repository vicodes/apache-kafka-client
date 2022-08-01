package com.vicodes.kafkaClient.core.api;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Getter
@Setter
@Builder
public class StreamsConfig {

  @Valid
  @NotNull
  private String bootstrapServers;
  @Valid
  @NotNull
  private int maxPolledRecords;
}
