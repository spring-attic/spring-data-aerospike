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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.convert.AerospikeData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.data.aerospike.repository.query.Criteria;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.utility.Utils;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueCallback;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;

/**
 * Primary implementation of {@link AerospikeOperations}.
 *  
 * @author Oliver Gierke
 * @author Peter Milne
 */
public class AerospikeTemplate implements AerospikeOperations {

	private static final MappingAerospikeConverter DEFAULT_CONVERTER = new MappingAerospikeConverter();
	private static final AerospikeExceptionTranslator DEFAULT_EXCEPTION_TRANSLATOR = new DefaultAerospikeExceptionTranslator();
	private final MappingContext<BasicAerospikePersistentEntity<?>, AerospikePersistentProperty> mappingContext;

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

		
		Assert.notNull(client, "Aerospike client must not be null!");
		Assert.notNull(namespace, "Namespace cannot be null");
		Assert.hasLength(namespace);

		this.client = client;
		this.converter = DEFAULT_CONVERTER;
		this.exceptionTranslator = DEFAULT_EXCEPTION_TRANSLATOR;
		this.namespace = namespace;
		this.mappingContext = new  AerospikeMappingContext();
		this.insertPolicy = new WritePolicy(this.client.writePolicyDefault);
		this.updatePolicy = new WritePolicy(this.client.writePolicyDefault);
		this.insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		this.updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

	}


	@Override
	public void insert(Serializable id, Object objectToInsert) {
		try {
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToInsert, data);
			data.setID(id);
			Key key = data.getKey();
			Bin[] bins = data.getBinsAsArray();
			client.put(this.insertPolicy, key, bins);
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.core.AerospikeOperations#save(java.io.Serializable, java.lang.Object, java.lang.Object)
	 */
	@Override
	public <T> void save(Serializable id, Object objectToInsert, Class<T> domainType) {
		try {
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToInsert, data);
			data.setID(id);
			data.setSetName(domainType.getSimpleName());
			Key key = data.getKey();
			Bin[] bins = data.getBinsAsArray();
			client.put(null, key, bins);
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
		
	}
	@Override
	public <T> T findById(Serializable id, Class<T> type, Class<T> domainType) {
		try {
			Key key = new Key(this.namespace, domainType.getSimpleName(), id.toString());
			AerospikeData data = AerospikeData.forRead(key, null);
			Record record = this.client.get(null, key);
			data.setRecord(record);
			return converter.read(type, data);
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}
	public void save(Serializable id, Object objectToInsert) {
		try {
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToInsert, data);
			data.setID(id);
			Key key = data.getKey();
			Bin[] bins = data.getBinsAsArray();
			client.put(null, key, bins);
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
			data.setID(id);
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
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(type, data);
			data.setID(id);
			this.client.delete(null, data.getKey());
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

	public <T> T add(T objectToAddTo, Map<String, Long> values) {
		try {
			
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToAddTo, data);
			Bin[] bins = new Bin[values.size()];
			int x = 0;
			for(Map.Entry<String, Long> entry : values.entrySet()){
				Bin newBin = new Bin(entry.getKey(), entry.getValue());
				bins[x] = newBin;
				x++;
			}
			this.client.add(null, data.getKey(), bins);
			
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
		return null;
	}

	public <T> T add(T objectToAddTo, String binName, int value) {
		try {
			
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToAddTo, data);
			this.client.add(null, data.getKey(), new Bin(binName, value));
			
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
		return null;
	}



	
	@Override
	public <T> T append(T objectToAppenTo, Map<String, String> values) {
		try {
			
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToAppenTo, data);
			Bin[] bins = new Bin[values.size()];
			int x = 0;
			for(Map.Entry<String, String> entry : values.entrySet()){
				Bin newBin = new Bin(entry.getKey(), entry.getValue());
				bins[x] = newBin;
				x++;
			}
			this.client.add(null, data.getKey(), bins);
			
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
		return null;
	}


	@Override
	public <T> T append(T objectToAppenTo, String binName, String value) {
		try {
			
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			converter.write(objectToAppenTo, data);
			this.client.add(null, data.getKey(), new Bin(binName, value));
			
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
		return null;
	}


	@Override
	public <T> List<T> findAll(final Class<T> type) {
		
		//TODO returning a list is dangerous because
		// the list is unbounded and could contain billions of elements
		// we need to find another solution
		final List<T> scanList = new ArrayList<T>();
		this.client.scanAll(null, this.namespace, this.getSetName(type), new ScanCallback() {
			
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				AerospikeData data = AerospikeData.forRead(key, null);
				data.setRecord(record);
				scanList.add(converter.read(type, data));
			}
		});
		return scanList;
	}

	@Override
	public <T> T findById(Serializable id, Class<T> type) {
		try {
			AerospikePersistentEntity<?> entity = converter.getMappingContext().getPersistentEntity(type);
			Key key = new Key(this.namespace, entity.getSetName(), id.toString());
			AerospikeData data = AerospikeData.forRead(key, null);
			Record record = this.client.get(null, key);
			data.setRecord(record);
			return converter.read(type, data);
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}
	@Override
	public <T> Iterable<T> aggregate(Filter filter, Class<?> type,
			Class<T> outputType, String module, String function, List<?> arguments) {
		Assert.notNull(filter, "Filter must not be null!");
		Assert.notNull(type, "Type must not be null!");
		Assert.notNull(outputType, "Output type must not be null!");

		AerospikePersistentEntity<?> entity = converter.getMappingContext().getPersistentEntity(type);

		Statement statement = new Statement();
		statement.setFilters(filter);
		statement.setSetName(entity.getSetName());
		
		ResultSet resultSet = this.client.queryAggregate(null, statement);
		
		return  (Iterable<T>) resultSet;
	}
	
	/**
	 * Configures the {@link AerospikeExceptionTranslator} to be used.
	 * 
	 * @param exceptionTranslator can be {@literal null}.
	 */
	public void setExceptionTranslator(AerospikeExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator == null ? DEFAULT_EXCEPTION_TRANSLATOR : exceptionTranslator;
	}

	





	/**
	 * {@link AerospikeClientCallback} to execute a query to return an {@link Iterable} of objects.
	 * 
	 * @author Oliver Gierke
	 * @author Peter Milne
	 */
	private static final class FindAllCallback<T> implements AerospikeClientCallback<Iterable<T>> {

		private final Class<T> type;
		private final Statement statement;
		private final AerospikeConverter converter;
		private String module;
		private String function;
		private List<Value> arguments;

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
		private FindAllCallback(Class<T> type, Statement statement, AerospikeConverter converter, String module, String function, List<Value> arguments) {

			this.type = type;
			this.statement = statement;
			this.converter = converter;
			this.module = module;
			this.function = function;
			this.arguments = arguments;
		}

		/**
		 * @param type2
		 * @param statement2
		 * @param defaultConverter
		 */
		public FindAllCallback(Class<T> type, Statement statement, MappingAerospikeConverter converter) {
			this.type = type;
			this.statement = statement;
			this.converter = converter;

		}
		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.aerospike.core.AerospikeClientCallback#doWith(com.aerospike.client.AerospikeClient)
		 */
		@Override
		public Iterable<T> recordIterator(AerospikeClient client)  {
			
			final List<T> returnList = new ArrayList<T>();

			RecordSet rs = client.query(null, statement);
			try {
				while (rs != null && rs.next()) {
					Record record = rs.getRecord();
					AerospikeData data = AerospikeData.forRead(rs.getKey(),
							null);
					data.setRecord(record);
					returnList.add(converter.read(type, data));
				}
			} finally {
				rs.close();
			}

			return (Iterable<T>) returnList;//TODO:create a sort
		}

		@Override
		public Iterable<T> resultIterator(AerospikeClient client)
				throws AerospikeException {
			final ResultSet resultSet = client.queryAggregate(null, statement, module, function, 
					(arguments==null || arguments.size() == 0)? null: arguments.toArray(new Value[arguments.size()]));

			
			return (Iterable<T>) resultSet.iterator();
		}
	}


	@Override
	public String getSetName(Class<?> entityClass) {
		AerospikePersistentEntity<?> entity = converter.getMappingContext().getPersistentEntity(entityClass);
		return entity.getSetName();
	}


	@Override
	public <T> Iterable<T> findAll(Sort sort, Class<T> type) {
		// TODO Auto-generated method stub
		return null;
	}



	private String resolveKeySpace(Class<?> type) {
		return this.mappingContext.getPersistentEntity(type).getSetName();
	}
	private static boolean typeCheck(Class<?> requiredType, Object candidate) {
		return candidate == null ? true : ClassUtils.isAssignable(requiredType, candidate.getClass());
	}

		
//		return execute(new KeyValueCallback<Iterable<T>>() {
//
//			@SuppressWarnings("unchecked")
//			@Override
//			public Iterable<T> doInKeyValue(KeyValueAdapter adapter) {
//
//				Iterable<?> result = adapter.find(query, resolveKeySpace(type));//this converted to filter somehoe
//				if (result == null) {
//					return Collections.emptySet();
//				}
//
//				List<T> filtered = new ArrayList<T>();
//
//				for (Object candidate : result) {
//					if (typeCheck(type, candidate)) {
//						filtered.add((T) candidate);
//					}
//				}
//
//				return filtered;
//			}
//		}); 
//	}


//	@Override
//	public <T> Iterable<T> findInRange(int offset, int rows, Class<T> type) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//
//	@Override
//	public <T> Iterable<T> findInRange(int offset, int rows, Sort sort,
//			Class<T> type) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//
//	@Override
//	public long count(Class<?> type) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//
//	@Override
//	public long count(KeyValueQuery<?> query, Class<?> type) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//
//	@Override
//	public MappingContext<?, ?> getMappingContext() {
//		// TODO Auto-generated method stub
//		return mappingContext;
//	}
//
//
//	@Override
//	public void destroy() throws Exception {
//		// TODO Auto-generated method stub
//		
//	}


	/* (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueOperations#execute(org.springframework.data.keyvalue.core.KeyValueCallback)
	 */
	@Override
	public <T> T execute(KeyValueCallback<T> action) {
		Assert.notNull(action, "KeyValueCallback must not be null!");

		try {
			return action.doInKeyValue(null);
		} catch (RuntimeException e) {
			throw e;
		}
	}



	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.core.AerospikeOperations#count(org.springframework.data.aerospike.repository.query.Query, java.lang.Class)
	 */
	@Override
	public int count(Query<?> query, Class<?> javaType) {
		// TODO Auto-generated method stub
		return 0;
	}


	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.core.AerospikeOperations#find(org.springframework.data.aerospike.repository.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> find(Query<?> query, Class<T> type) {
		Assert.notNull(query, "Filter must not be null!");
		Assert.notNull(type, "Type must not be null!");
		Criteria criteria = (Criteria) query.getCritieria();
		Filter filter = query.getQueryObject();

		AerospikePersistentEntity<?> entity = converter.getMappingContext().getPersistentEntity(type);
		
		AerospikeData data = AerospikeData.forWrite(this.namespace);
		try {
			converter.write(type.newInstance(), data);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] bins = {"firstname","lastname"};
		Statement statement = new Statement();
		statement.setNamespace(this.namespace);
		statement.setFilters(filter);
		statement.setSetName(entity.getSetName());
		statement.setBinNames(data.getBinNames());
		
		FindAllCallback<T> callBack = new FindAllCallback<T>(type, statement, DEFAULT_CONVERTER);
		
		
		
		return callBack.recordIterator(client);
	}


	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.core.AerospikeOperations#getMappingContext()
	 */
	@Override
	public MappingContext<?, ?> getMappingContext() {
		return this.mappingContext;
	}


	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueOperations#findInRange(int, int, org.springframework.data.domain.Sort, java.lang.Class)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public <T> Iterable<T> findInRange(int offset, int rows, Sort sort, Class<T> type) {
		return find(new Query(sort).skip(offset).limit(rows), type);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueOperations#count(java.lang.Class)
	 */
	@Override
	public long count(Class<?> type) {

		Assert.notNull(type, "Type for count must not be null!");
		return 0;
	}

}
