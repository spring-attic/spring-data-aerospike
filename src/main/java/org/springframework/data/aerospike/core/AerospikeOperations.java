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
import java.util.List;
import java.util.Map;

import javax.management.Query;

import org.springframework.data.aerospike.core.aggregation.AggregationResults;

import com.aerospike.client.query.Filter;

/**
 * Aerospike specific data access operations.
 * 
 * @author Oliver Gierke
 * @author Peter Milne
 */
public interface AerospikeOperations {

	/**
	 * The Set name used for the specified class by this template.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	String getSetName(Class<?> entityClass);
	
	/**
	 * Insert operation using the WritePolicy.recordExisits policy of CREATE_ONLY 
	 * @param objectToInsert
	 * @return
	 */
	public <T> T insert(T objectToInsert);
	/**
	 * Insert operation using the WritePolicy.recordExisits policy of CREATE_ONLY 
	 * @param id
	 * @param objectToInsert
	 */
	void insert(Serializable id, Object objectToInsert);
	
	
	void update(Object objectToUpdate);
	void update(Serializable id, Object objectToUpdate);
	
	void delete(Class<?> type);
	
	<T> T delete(Serializable id, Class<T> type);
	<T> T delete(T objectToDelete);
	
	<T> Iterable<T> find(Filter filter, Class<T> entityClass);
	<T> List<T> findAll(Class<T> type);
	<T> T findById(Serializable id, Class<T> type);
	
	<T> T add(T objectToAddTo, Map<String, Long> values);
	<T> T add(T objectToAddTo, String binName, int value);

	<T> T append(T objectToAppenTo, Map<String, String> values);
	<T> T append(T objectToAppenTo, String binName, String value);
	
	<T> Iterable<T> aggregate(Filter filter, Class<?> type, Class<T> outputType, String module, String function, List<?> arguments);
	

	/**
	 * Returns all entities of the given type matching the filter given {@link Filter}.
	 * 
	 * @param filter must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return
	 */
	<T> Iterable<T> findAll(Filter filter, Class<T> type);
	
}
