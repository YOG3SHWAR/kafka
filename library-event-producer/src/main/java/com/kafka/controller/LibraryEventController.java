package com.kafka.controller;

import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.domain.LibraryEvent;
import com.kafka.domain.LibraryEventType;
import com.kafka.producer.LibraryEventProducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@RestController()
@RequestMapping("/v1")
@Slf4j
public class LibraryEventController {

    @Autowired
    LibraryEventProducer eventProducer;

    @PostMapping("/library-event")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException {

        // invoking kafka producer
        log.info("before sendLibraryEvent");
        eventProducer.sendLibraryEvent(libraryEvent);
        log.info("after sendLibraryEvent");
        // this is an asynchronous call, this will return status 201 (CREATED) even
        // if the message is not published

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PostMapping("/library-event-sync")
    public ResponseEntity<LibraryEvent> postLibraryEventSynchronous(@RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException, InterruptedException, ExecutionException {

        // invoking kafka producer
        // this is a synchronous call
        log.info("before sendLibraryEventSynchronous");
        eventProducer.sendLibraryEventSynchronous(libraryEvent);
        log.info("after sendLibraryEventSynchronous");

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PostMapping("/library-event2")
    public ResponseEntity<LibraryEvent> postLibraryEvent2(@RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException {

        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        // invoking kafka producer using send() and ProducerRecord
        eventProducer.sendLibraryEvent2(libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
