package com.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.domain.LibraryEvent;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);

            }

            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);

            }

            private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {

                log.info("Message sent successfully, key = {}, value = {}, partition = {}", key, value,
                        result.getRecordMetadata().partition());

            }

            private void handleFailure(Integer key, String value, Throwable ex) {
                log.error("Error sending the message , key = {}, value = {}, message = {}", key, value,
                        ex.getMessage());
                try {
                    throw ex;
                } catch (Throwable e) {
                    log.error("Error in onFailure: {}", e.getMessage());
                }
            }
        });
    }
}