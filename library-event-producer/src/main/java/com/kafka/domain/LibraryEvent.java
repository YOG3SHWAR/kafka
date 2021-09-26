package com.kafka.domain;

import lombok.Data;

@Data
public class LibraryEvent {
    private int libraryEventId;
    private Book book;
}
