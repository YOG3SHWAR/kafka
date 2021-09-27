package com.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.service.LibraryEventService;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventsConsumer {

    @Autowired
    LibraryEventService service;

    @KafkaListener(topics = { "library-events" })
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("ConsumerRecord = {}", consumerRecord);
        service.processLibraryEvent(consumerRecord);
    }

}
