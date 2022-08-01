package com.vicodes.kafkaClient.core.kafka.factory;

import com.vicodes.kafkaClient.core.api.IConsumerRecordHandler;
import com.vicodes.kafkaClient.core.api.ConsumerScheduledTaskConfig;
import com.vicodes.kafkaClient.core.api.StreamsConfig;
import com.vicodes.kafkaClient.core.kafka.worker.StreamConsumerWorker;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConditionalOnBean(value = StreamsConfig.class)
public class StreamConsumerWorkerFactory {
  public StreamConsumerWorker get(final ConsumerScheduledTaskConfig consumerScheduledTaskConfig,
                                  final StreamsConfig streamsConfig,
                                  final Map<String, IConsumerRecordHandler> consumerRecordHandlerMap) {

    return new StreamConsumerWorker(consumerScheduledTaskConfig, streamsConfig, consumerRecordHandlerMap);
  }
}
