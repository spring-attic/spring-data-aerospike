package org.springframework.data.aerospike.repository;

import java.io.Serializable;

import org.springframework.data.repository.CrudRepository;

public interface AerospikeRepository<T, ID extends Serializable> extends CrudRepository<T, ID> {

}
