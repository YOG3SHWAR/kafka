package com.kafka.service;

import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.LibraryEvent;
import com.kafka.jpa.LibraryEventRepository;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
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

        if(libraryEvent.getLibraryEventId()!=null && libraryEvent.getLibraryEventId()==000)
            throw new RecoverableDataAccessException("Network Issue");

        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid library event type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {

        if (libraryEvent.getLibraryEventId() == null)
            throw new IllegalArgumentException("Library Event Id is missing");

        Optional<LibraryEvent> optional = repository.findById(libraryEvent.getLibraryEventId());

        if (!optional.isPresent())
            throw new IllegalArgumentException("Invalid Library Event");

        log.info("Validation Successful");
    }

    private void save(LibraryEvent libraryEvent) {

        repository.save(libraryEvent);
        log.info("Successfully saved in db");
    }
}
