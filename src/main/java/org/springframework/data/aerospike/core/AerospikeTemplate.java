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

import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.convert.AerospikeData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.util.Assert;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
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

	private AerospikeExceptionTranslator exceptionTranslator;

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
				result.add(converter.read(type, AerospikeData.forRead(recordSet.getKey(), recordSet.getRecord())));
			}

			return result;
		}
	}
}
