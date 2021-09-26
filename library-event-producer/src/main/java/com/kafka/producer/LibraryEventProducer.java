package com.kafka.producer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.domain.LibraryEvent;

import org.apache.kafka.clients.producer.ProducerRecord;
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

    String topic = "library-events";

    // this is an asynchronous function, it will immediately return
    // listenableFuture, and handleSuccess will run in different thread
    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        // sendDefault will send msg to default topic (configured in application.yml)
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }

            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

        });
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent)
            throws InterruptedException, ExecutionException, JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> result = null;
        try {
            result = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
            // this get() will make this fn to wait untill the message
            // is published, it can be used without the timeout also
        } catch (InterruptedException | ExecutionException e) {
            log.error(
                    "InterruptedException | ExecutionException Error sending the message , key = {}, value = {}, message = {}",
                    key, value, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception sending the message , key = {}, value = {}, message = {}", key, value, e.getMessage());
        }

        return result;
    }

    public void sendLibraryEvent2(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
        // sendDefault will send msg to default topic (configured in application.yml)
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }

            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

        });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic2) {
        return new ProducerRecord<>(topic, null, key, value, null);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {

        log.info("Message sent successfully, key = {}, value = {}, partition = {}", key, value,
                result.getRecordMetadata().partition());

    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending the message , key = {}, value = {}, message = {}", key, value, ex.getMessage());
        try {
            throw ex;
        } catch (Throwable e) {
            log.error("Error in onFailure: {}", e.getMessage());
        }
    }
}
