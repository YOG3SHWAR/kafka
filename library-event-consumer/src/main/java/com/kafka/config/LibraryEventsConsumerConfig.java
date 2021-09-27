package com.kafka.config;

import java.util.HashMap;
import java.util.Map;

import javax.management.RuntimeErrorException;

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);

        factory.setConcurrency(3); // setting number of concurrent listners
        // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL)
        factory.setErrorHandler(((thrownException, data) -> {
            log.info("Exception in consumer config = {}, record = {}", thrownException.getMessage(), data);
        }));

        // retry
        factory.setRetryTemplate(retryTemplate());

        // recovery
        factory.setRecoveryCallback((context -> {
            if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
                // recovery logic
                log.info("message is recoverable, inside recoevery logic");
            } else {
                throw new RuntimeException(context.getLastThrowable().getMessage());
            }
            return null;
        }));

        return factory;
    }

    private RetryTemplate retryTemplate() {

        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(1000); // 1000ms
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setBackOffPolicy(backOffPolicy);
        retryTemplate.setRetryPolicy(retryPolicy());
        return retryTemplate;
    }

    private RetryPolicy retryPolicy() {

        Map<Class<? extends Throwable>, Boolean> exceptionsMap = new HashMap<>();
        exceptionsMap.put(IllegalArgumentException.class, false);
        exceptionsMap.put(RecoverableDataAccessException.class, true);
        return new SimpleRetryPolicy(3, exceptionsMap, true);
    }
}
