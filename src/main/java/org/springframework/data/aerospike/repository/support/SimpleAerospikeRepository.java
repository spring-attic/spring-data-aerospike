package org.springframework.data.aerospike.repository.support;

import java.io.Serializable;

import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.repository.support.SimpleKeyValueRepository;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.support.PersistentEntityInformation;
import org.springframework.util.Assert;

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
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findOne(java.io.Serializable)
	 */
	@Override
	public T findOne(ID id) {
		return operations.findById(id, entityInformation.getJavaType(), getDomainClass());
	}
	@Override
	public <S extends T> S save(S entity) {
		Assert.notNull(entity);
		operations.save(entityInformation.getId(entity), entity, getDomainClass());
		
		//operations.save(entityInformation.getId(entity), entity,entityInformation.get);
		return entity;
	}
	
	@Override
	public void delete(T entity) {
		operations.delete(entity);
	}

	/**
	 * @return the entityInformation
	 */
	public Class<T> getDomainClass() {
		return  this.entityInformation.getJavaType();
	}
	
	
}
