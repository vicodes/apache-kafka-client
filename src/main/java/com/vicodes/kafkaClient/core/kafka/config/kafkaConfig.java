package com.vicodes.kafkaClient.core.kafka.config;

import com.vicodes.kafkaClient.core.api.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import static com.vicodes.kafkaClient.core.api.Constants.*;

@Configuration
@ConditionalOnProperty(value="kafka.bootstrap.server")
public class kafkaConfig {
    @Autowired
    Environment environment;

    @Bean
    public StreamsConfig streamsConfig(){
        return StreamsConfig.builder()
                .bootstrapServers(environment.getProperty(KAFKA_BOOTSTRAP_SERVER))
                .maxPolledRecords(environment.getProperty(KAFKA_MAX_POLL_RECORDS, Integer.class, DEFAULT_KAFKA_MAX_POLL_RECORDS))
                .build();
    }
}
