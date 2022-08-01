package com.vicodes.kafkaClient.core.api;

import com.vicodes.kafkaClient.core.kafka.worker.StreamConsumerWorker;

import java.util.List;

public interface IConsumerManager {

    void start() throws Exception;

    void stop() throws Exception;

    void restart() throws Exception;

    StreamConsumerWorker getConsumerViaInstanceName(String consumerInstanceName);

    List<StreamConsumerWorker> getConsumerViaGroup(String consumerGroupName);
}
