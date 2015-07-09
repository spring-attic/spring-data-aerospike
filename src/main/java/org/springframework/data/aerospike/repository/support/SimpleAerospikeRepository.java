package org.springframework.data.aerospike.repository.support;

import java.io.Serializable;

import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.repository.support.SimpleKeyValueRepository;
import org.springframework.data.repository.core.EntityInformation;

public class SimpleAerospikeRepository<T, ID extends Serializable> extends
		SimpleKeyValueRepository<T, ID> {

	private final AerospikeOperations operations;
	private final EntityInformation<T, ID> entityInformation;

	public SimpleAerospikeRepository(EntityInformation<T, ID> metadata,
			KeyValueOperations operations) {
		super(metadata, operations);
		this.entityInformation = metadata;
		this.operations = (AerospikeOperations) operations;
	}

	@Override
	public <S extends T> S save(S entity) {
		operations.save(entityInformation.getId(entity), entity);
		return entity;
	}
	
	@Override
	public void delete(T entity) {
		operations.delete(entity);
	}
}
