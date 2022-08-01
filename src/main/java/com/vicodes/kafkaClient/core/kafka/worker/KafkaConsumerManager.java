package com.vicodes.kafkaClient.core.kafka.worker;


import com.vicodes.kafkaClient.core.api.IConsumerManager;
import com.vicodes.kafkaClient.core.api.IConsumerRecordHandler;
import com.vicodes.kafkaClient.core.api.ConsumerScheduledTaskConfig;
import com.vicodes.kafkaClient.core.api.StreamsConfig;
import com.vicodes.kafkaClient.core.kafka.factory.StreamConsumerWorkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Component
@ConditionalOnBean(value = StreamsConfig.class)
public class KafkaConsumerManager implements IConsumerManager {

    private final StreamsConfig streamsConfig;
    private final StreamConsumerWorkerFactory streamConsumerWorkerFactory;
    private final List<StreamConsumerWorker> consumerWorkers;
    private final List<ConsumerScheduledTaskConfig> consumerScheduledTaskConfigs;
    private Map<String, IConsumerRecordHandler> consumerRecordHandlerMap;

    private final static Integer DEFAULT_INSTANCE_COUNT = 1;

    @Autowired
    public KafkaConsumerManager(final StreamsConfig streamsConfig,
                                final StreamConsumerWorkerFactory streamConsumerWorkerFactory,
                                final List<StreamConsumerWorker> consumerWorkers,
                                final List<ConsumerScheduledTaskConfig> consumerScheduledTaskConfigs,
                                final Map<String, IConsumerRecordHandler> consumerRecordHandlerMap) {
        this.streamsConfig = streamsConfig;
        this.streamConsumerWorkerFactory = streamConsumerWorkerFactory;
        this.consumerWorkers = consumerWorkers;
        this.consumerScheduledTaskConfigs = consumerScheduledTaskConfigs;
        this.consumerRecordHandlerMap = consumerRecordHandlerMap;
    }

    @PostConstruct
    @Override
    public void start() throws Exception {
        this.createConsumers();
        this.consumerWorkers.forEach((periodicWorker) -> {
            periodicWorker.startAsync().awaitRunning();
        });
    }
    @PreDestroy
    @Override
    public void stop() throws Exception {
        log.info("going to stop the consumers");
        consumerWorkers.stream().forEach(streamConsumerWorker -> {
            streamConsumerWorker.close();
        });
        consumerWorkers.clear();
    }

    @Override
    public void restart() throws Exception {
        stop();
        start();
    }

    @Override
    public StreamConsumerWorker getConsumerViaInstanceName(String consumerInstanceName) {

        return consumerWorkers.stream()
                .filter(streamConsumerWorker -> streamConsumerWorker.getConsumerInstanceName().equals(consumerInstanceName))
                .findFirst().get();
    }

    @Override
    public List<StreamConsumerWorker> getConsumerViaGroup(String consumerGroupName) {
        return consumerWorkers.stream()
                .filter(streamConsumerWorker -> streamConsumerWorker.getConsumerGroupName().equals(consumerGroupName))
                .collect(Collectors.toList());
    }
    private void createConsumers() {
        consumerScheduledTaskConfigs.stream()
                .filter(consumerScheduledTaskConfig -> consumerScheduledTaskConfig.isActive())
                .forEach(consumerScheduledTaskConfig ->
                        IntStream.range(0, consumerScheduledTaskConfig.getInstanceCount())
                                .forEach(n ->
                                        consumerWorkers.add(streamConsumerWorkerFactory.get(consumerScheduledTaskConfig, streamsConfig, consumerRecordHandlerMap)))
                );
    }

}
