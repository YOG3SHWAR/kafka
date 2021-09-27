package com.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.LibraryEvent;
import com.kafka.jpa.LibraryEventRepository;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventService {
    @Autowired
    ObjectMapper mapper;

    @Autowired
    LibraryEventRepository repository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = mapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent = {}", libraryEvent);

        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                // update in db
                break;
            default:
                log.info("Invalid library event type");
        }
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);
        log.info("Successfully saved in db");
    }
}
