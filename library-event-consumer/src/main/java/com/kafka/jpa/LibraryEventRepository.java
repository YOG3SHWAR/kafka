package com.kafka.jpa;

import com.kafka.entity.LibraryEvent;

import org.springframework.data.repository.CrudRepository;

public interface LibraryEventRepository extends CrudRepository<LibraryEvent, Integer> {

}
