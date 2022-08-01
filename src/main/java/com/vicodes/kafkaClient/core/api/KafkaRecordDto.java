package com.vicodes.kafkaClient.core.api;

import com.vicodes.kafkaClient.core.kafka.dto.KafkaValueDto;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class KafkaRecordDto implements Serializable {
    @JsonProperty("KEY")
    private String key;
    @JsonProperty("TYPE")
    private String type;
    @JsonProperty("VALUE")
    private KafkaValueDto value;

}