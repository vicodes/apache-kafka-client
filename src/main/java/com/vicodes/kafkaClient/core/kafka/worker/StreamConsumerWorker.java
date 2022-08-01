package com.vicodes.kafkaClient.core.kafka.worker;

import com.vicodes.kafkaClient.core.api.IConsumerRecordHandler;
import com.vicodes.kafkaClient.core.api.KafkaProcessInfo;
import com.vicodes.kafkaClient.core.api.ConsumerScheduledTaskConfig;
import com.vicodes.kafkaClient.core.api.StreamsConfig;
import com.vicodes.kafkaClient.core.kafka.dto.KafkaValueDto;
import com.vicodes.kafkaClient.core.kafka.serialization.ValueDtoDeserializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.AbstractScheduledService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Getter
public class StreamConsumerWorker extends AbstractScheduledService {

    private ConsumerScheduledTaskConfig consumerScheduledTaskConfig;
    private StreamsConfig streamsConfig;
    private Map<String, IConsumerRecordHandler> consumerRecordHandlerMap;
    private Consumer<String, KafkaValueDto> consumer;
    private final Long periodOfConsumptionInMillis;
    private final String consumerInstanceName;
    private final String consumerGroupName;
    private final static String LOG_FORMAT = "consumed on kafka with  topic = {}, partition = {}, offset= {}, data = {}, key= {}";

    public StreamConsumerWorker(final ConsumerScheduledTaskConfig consumerScheduledTaskConfig,
                                final StreamsConfig streamsConfig,
                                final Map<String, IConsumerRecordHandler> consumerRecordHandlerMap) {
        this.consumerRecordHandlerMap = consumerRecordHandlerMap;
        this.streamsConfig = streamsConfig;
        this.consumerScheduledTaskConfig = consumerScheduledTaskConfig;
        this.periodOfConsumptionInMillis = consumerScheduledTaskConfig.getPeriodOfConsumptionInMillis();
        this.consumerInstanceName = consumerScheduledTaskConfig.getConsumerGroup() + "_" + UUID.randomUUID().toString();
        this.consumerGroupName = consumerScheduledTaskConfig.getConsumerGroup();
        this.consumer = createConsumer();
    }

    private Consumer<String, KafkaValueDto> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, streamsConfig.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.consumerGroupName);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ValueDtoDeserializer.class.getName());
         props.put(ConsumerConfig.CLIENT_ID_CONFIG, this.consumerInstanceName);
        if (Objects.nonNull(consumerScheduledTaskConfig.getMaxPollRecords())) {
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerScheduledTaskConfig.getMaxPollRecords());
        }
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (Objects.nonNull(consumerScheduledTaskConfig.getMaxBytes())) {
            props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, consumerScheduledTaskConfig.getMaxBytes());
        }
        final Consumer<String, KafkaValueDto> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    private void processRecord(final ConsumerRecord<String, KafkaValueDto> record) {
        log.info(LOG_FORMAT, consumerScheduledTaskConfig.getTopic(), record.partition(), record.offset(), record.value(), record.key());
        KafkaValueDto kafkaConsumedRecord = record.value();
        decorateKafkaValueDto(kafkaConsumedRecord);

        try {
            KafkaProcessInfo kafkaProcessInfo = consume(kafkaConsumedRecord);
            consumer.commitAsync();
            decorateKafkaProcessInfo(kafkaProcessInfo, record, consumerScheduledTaskConfig);
            log.info(kafkaProcessInfo.toString());
        } catch (JsonProcessingException e) {
            log.error("Error occurred while processing the message in {}", e);
        } catch (InterruptedException e) {
            log.error("Error occurred while processing the message in {}", e);
        } catch (ExecutionException e) {
            log.error("Error occurred while processing the message in {}", e);
        } catch (Exception e) {
            log.error("Error occurred while processing the message in {}", e);
        }

    }

    private void decorateKafkaProcessInfo(final KafkaProcessInfo kafkaProcessInfo, final ConsumerRecord<String, KafkaValueDto> record, final ConsumerScheduledTaskConfig consumerScheduledTaskConfig) {
        kafkaProcessInfo.setOffset(record.offset());
        kafkaProcessInfo.setPartition(record.partition());
        kafkaProcessInfo.setConsumerGroup(consumerScheduledTaskConfig.getConsumerGroup());
        kafkaProcessInfo.setTopic(consumerScheduledTaskConfig.getTopic());
    }

    private void decorateKafkaValueDto(final KafkaValueDto kafkaValueDto) {
        if (Objects.isNull(kafkaValueDto)) {
            return;
        }
        ObjectNode meta = kafkaValueDto.getMeta();
        meta.put("task", consumerScheduledTaskConfig.getTaskName());
        meta.put("consumer", consumerScheduledTaskConfig.getConsumerGroup());
        meta.put("topic", consumerScheduledTaskConfig.getTopic());

    }

    private KafkaProcessInfo consume(final KafkaValueDto kafkaConsumedRecord) throws Exception {
        Instant start = Instant.now();
        IConsumerRecordHandler recordHandler = consumerRecordHandlerMap.get(consumerScheduledTaskConfig.getTaskName());
        if (recordHandler == null)
            log.error("No handler class is mapped to consumer task :{}", consumerScheduledTaskConfig.getTaskName());

        return KafkaProcessInfo.builder()
                .consumedSuccessfully(recordHandler.execute(kafkaConsumedRecord))
                .processingTime(Duration.between(start, Instant.now()).toMillis()).build();
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, periodOfConsumptionInMillis, TimeUnit.MILLISECONDS);

    }

    @Override
    protected void shutDown() {
        log.info("consumer shutdown: {}", this.consumerInstanceName);
        consumer.unsubscribe();
    }

    @Override
    protected void runOneIteration() throws Exception {
        Duration duration = Duration.ofMillis(1000);
        try {
            final ConsumerRecords<String, KafkaValueDto> consumerRecords = consumer.poll(duration);
            consumerRecords.forEach(record -> processRecord(record));
        } catch (Exception e) {
            log.error("error = {}", e);
        }
    }

    @Override
    protected void startUp() {
        log.info("consumer starting {}", this.consumerInstanceName);
        consumer.subscribe(Collections.singletonList(consumerScheduledTaskConfig.getTopic()));
    }

    public void close() {
        try {
            this.stopAsync().awaitTerminated(1, TimeUnit.SECONDS);
            consumer.close();
            if (consumer != null) {
                consumer.close();
            }
        } catch (Exception e) {
            log.error("Exception while close {}", e);
        }
    }
}
