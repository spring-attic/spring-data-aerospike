/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.convert.AerospikeData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.utility.Utils;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.util.Assert;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

/**
 * Primary implementation of {@link AerospikeOperations}.
 * 
 * @author Oliver Gierke
 */
public class AerospikeTemplate extends KeyValueTemplate implements AerospikeOperations {

	private static final MappingAerospikeConverter DEFAULT_CONVERTER = new MappingAerospikeConverter();
	private static final AerospikeExceptionTranslator DEFAULT_EXCEPTION_TRANSLATOR = new DefaultAerospikeExceptionTranslator();

	private final AerospikeClient client;
	private final MappingAerospikeConverter converter;
	private final String namespace;



	private AerospikeExceptionTranslator exceptionTranslator;
	private WritePolicy insertPolicy;
	private WritePolicy updatePolicy;

	/**
	 * Creates a new {@link AerospikeTemplate} for the given {@link AerospikeClient}.
	 * 
	 * @param client must not be {@literal null}.
	 */
	public AerospikeTemplate(AerospikeClient client, String namespace) {

		super(new AerospikeKeyValueAdapter(client, DEFAULT_CONVERTER, namespace));

		Assert.notNull(client, "Aerospike client must not be null!");

		this.client = client;
		this.converter = DEFAULT_CONVERTER;
		this.exceptionTranslator = DEFAULT_EXCEPTION_TRANSLATOR;
		this.namespace = namespace;
		this.insertPolicy = new WritePolicy(this.client.writePolicyDefault);
		this.updatePolicy = new WritePolicy(this.client.writePolicyDefault);
		this.insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		this.updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

	}


	@Override
	public void insert(Serializable id, Object objectToInsert) {
		try {
			//TODO use passed in ID
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToInsert, data);
			Key key = data.getKey();
			Bin[] bins = data.getBinsAsArray();
			client.put(this.insertPolicy, key, bins);
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}

	@Override
	public <T> T insert(T objectToInsert) {
		try{
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToInsert, data);
			Key key = data.getKey();
			Bin[] bins = data.getBinsAsArray();  
			client.put(this.insertPolicy, key, bins);
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
		return null;
	}

	@Override
	public void update(Object objectToUpdate) {
		try {
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToUpdate, data);
			Key key = data.getKey();
			Bin[] bins = data.getBinsAsArray();
			client.put(this.updatePolicy, key, bins);
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}

	@Override
	public void update(Serializable id, Object objectToUpdate) {
		try {
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToUpdate, data);
			client.put(this.updatePolicy, data.getKey(), data.getBinsAsArray());
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}

	@Override
	public void delete(Class<?> type) {
		try {
			//"set-config:context=namespace;id=namespace_name;set=set_name;set-delete=true;"
			Utils.infoAll(client, "set-config:context=namespace;id=" + this.namespace + ";set=" + type.getSimpleName() + ";set-delete=true;");
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}

	@Override
	public <T> T delete(Serializable id, Class<T> type) {
		try {
			this.client.delete(null, new Key(this.namespace, type.getSimpleName(), id.toString()));
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
		return null;
	}

	@Override
	public <T> T delete(T objectToDelete) {
		try {
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToDelete, data);
			this.client.delete(null, data.getKey());
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
		return null;
	}

	@Override
	public long count(Class<?> type) {
		// TODO Auto-generated method stub
		return super.count(type);
	}

	@Override
	public long count(KeyValueQuery<?> query, Class<?> type) {
		// TODO Auto-generated method stub
		return super.count(query, type);
	}

	@Override
	public <T> List<T> find(KeyValueQuery<?> query, Class<T> type) {
		// TODO Auto-generated method stub
		return super.find(query, type);
	}
	@Override
	public <T> List<T> findAll(Class<T> type) {
		// TODO Auto-generated method stub
		return super.findAll(type);
	}
	@Override
	public <T> List<T> findAll(Sort sort, Class<T> type) {
		// TODO Auto-generated method stub
		return super.findAll(sort, type);
	}
	@Override
	public <T> T findById(Serializable id, Class<T> type) {
		try {
			Key key = new Key(this.namespace, type.getSimpleName(), id.toString());
			AerospikeData data = AerospikeData.forRead(key, null);
			Record record = this.client.get(null, data.getKey(), data.getBinNames());
			data.setRecord(record);
			return converter.read(type, data);
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}
	/**
	 * Configures the {@link AerospikeExceptionTranslator} to be used.
	 * 
	 * @param exceptionTranslator can be {@literal null}.
	 */
	public void setExceptionTranslator(AerospikeExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator == null ? DEFAULT_EXCEPTION_TRANSLATOR : exceptionTranslator;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.aerospike.core.AerospikeOperations#query(com.aerospike.client.query.Filter, java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAll(Filter filter, final Class<T> type) {

		Assert.notNull(filter, "Filter must not be null!");
		Assert.notNull(type, "Type must not be null!");

		AerospikePersistentEntity<?> entity = converter.getMappingContext().getPersistentEntity(type);

		Statement statement = new Statement();
		statement.setFilters(filter);
		statement.setSetName(entity.getSetName());

		return execute(new FindAllCallback<T>(type, statement, converter));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.aerospike.core.AerospikeOperations#execute(org.springframework.data.aerospike.core.AerospikeClientCallback)
	 */
	@Override
	public <T> T execute(AerospikeClientCallback<T> callback) {

		Assert.notNull(callback, "AerospikeClientCallback must not be null!");

		try {
			return callback.doWith(client);
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueTemplate#resolveKeySpace(java.lang.Class)
	 */
	@Override
	protected String resolveKeySpace(Class<?> type) {

		AerospikePersistentEntity<?> entity = converter.getMappingContext().getPersistentEntity(type);
		return entity.getSetName();
	}


	/**
	 * {@link AerospikeClientCallback} to execute a query to return an {@link Iterable} of objects.
	 * 
	 * @author Oliver Gierke
	 */
	private static final class FindAllCallback<T> implements AerospikeClientCallback<Iterable<T>> {

		private final Class<T> type;
		private final Statement statement;
		private final AerospikeConverter converter;

		/**
		 * Creates a new {@link FindAllCallback} for the given type, {@link Statement} and {@link AerospikeConverter}.
		 * 
		 * @param type must not be {@literal null}.
		 * @param statement must not be {@literal null}.
		 * @param converter must not be {@literal null}.
		 */
		private FindAllCallback(Class<T> type, Statement statement, AerospikeConverter converter) {

			this.type = type;
			this.statement = statement;
			this.converter = converter;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.aerospike.core.AerospikeClientCallback#doWith(com.aerospike.client.AerospikeClient)
		 */
		@Override
		public Iterable<T> doWith(AerospikeClient client) throws AerospikeException {

			RecordSet recordSet = client.query(null, statement);
			List<T> result = new ArrayList<T>();

			while (recordSet.next()) {
				//result.add(converter.read(type, AerospikeData.forRead(recordSet.getKey(), recordSet.getRecord())));
			}

			return result;
		}
	}
}
