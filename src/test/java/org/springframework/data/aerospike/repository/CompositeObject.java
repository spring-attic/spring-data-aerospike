package org.springframework.data.aerospike.repository;

import lombok.Builder;
import lombok.Value;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;

@Builder
@Value
@Document
public class CompositeObject {

    @Id
    String id;
    SimpleObject simpleObject;

    @PersistenceConstructor
    public CompositeObject(String id, SimpleObject simpleObject) {
        this.id = id;
        this.simpleObject = simpleObject;
    }

}