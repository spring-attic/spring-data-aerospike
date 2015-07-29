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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.KeyValueCallback;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.util.CloseableIterator;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

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
	private int count = 0;



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
	@Override
	public <T> void createIndex(Class<T> domainType,String indexName,String binName, IndexType indexType  ){

		IndexTask task =  client.createIndex(null, this.namespace, domainType.getSimpleName(), indexName, binName, indexType);
		task.waitTillComplete();
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
			ScanPolicy scanPolicy = new ScanPolicy();
			scanPolicy.includeBinData = false;
			final AtomicLong count = new AtomicLong();
			client.scanAll(scanPolicy, namespace,  type.getSimpleName(), new ScanCallback() {

				
				@Override
				public void scanCallback(Key key, Record record)
						throws AerospikeException {


					if (client.delete(null, key)) 
						count.addAndGet(1);
			           /*
			            * after 25,000 records delete, return print the count.
			            */
			           if (count.get() % 10000 == 0){
			               System.out.println("Deleted "+ count.get());
			           }
					
				}
			   }, new String[] {});
			System.out.println("Deleted "+ count + " records from set " + type.getSimpleName());
		} catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}

	@Override
	public <T> T delete(Serializable id, Class<T> type) {
		try {
			AerospikeData data = AerospikeData.forWrite(this.namespace);
			data.setID(id);
			data.setSetName(type.getSimpleName());
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
		Iterable<T> results = findAllUsingQuery(type, null);
		Iterator<T> iterator = results.iterator();
		try {
			while (iterator.hasNext()){
				scanList.add(iterator.next());
			}
		} finally {
			((EntityIterator<T>)iterator).close();
		}
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
	@SuppressWarnings("unchecked")
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
		// TODO Invoke aggregation to count
		return 0;
	}


	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.core.AerospikeOperations#find(org.springframework.data.aerospike.repository.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> find(Query<?> query, Class<T> type) {
		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(type, "Type must not be null!");
		Criteria criteria = (Criteria) query.getCritieria();
		Filter filter = query.getQueryObject();
		
		return findAllUsingQuery(type, filter);
	}


	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.core.AerospikeOperations#getMappingContext()
	 */
	@Override
	public MappingContext<?, ?> getMappingContext() {
		return this.mappingContext;
	}


	public String getNamespace() {
		return namespace;
	}


	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueOperations#findInRange(int, int, org.springframework.data.domain.Sort, java.lang.Class)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public <T> Iterable<T> findInRange(int offset, int rows, Sort sort, Class<T> type) {
		System.out.println("skiping first " + offset + " and returning " + rows + " rows");
		final long rowCount = rows;
		final AtomicLong count = new AtomicLong(0);
		final Iterable<T> results = findAllUsingQuery(type, null);
		final Iterator<T> iterator = results.iterator();
		/*
		 * skip over offset
		 */
		
		for (int skip = 0; skip < offset; skip++){
			if (iterator.hasNext())
				iterator.next();
		}
		/*
		 * setup the iterable litimed by 'rows'
		 */

		Iterable<T> returnList = new Iterable<T>() {

			@Override
			public Iterator<T> iterator() {
				return new Iterator<T>() {

					@Override
					public boolean hasNext() {
						if (count.get() == rowCount){
							((EntityIterator<T>)iterator).close();
							return false;
						} else {
							return iterator.hasNext();
						}
					}

					@Override
					public T next() {
						if (count.addAndGet(1) <= rowCount){
							return iterator.next();
						} else {
							return null;
						}
					}
				};
			}
		};
		return (Iterable<T>) returnList;//TODO:create a sort
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueOperations#count(java.lang.Class)
	 */
	@Override
	public long count(Class<?> type, String setName) {
		Assert.notNull(type, "Type for count must not be null!");
		String answer = Info.request(null, client.getNodes()[0], "sets");
		String answer2 = Info.request(null, client.getNodes()[0], "namespaces");
		Node[] nodes = client.getNodes();
		int replicationCount = 2;  //TODO
		int nodeCount = nodes.length;
		int n_objects = 0;
		for (Node node : nodes){
			// Invoke an info call to each node in the cluster and sum the n_objects value
			// The infoString will contain a result like this:
			// n_objects=100001:set-stop-write-count=0:set-evict-hwm-count=0:set-enable-xdr=use-default:set-delete=false;
			String infoString = Info.request(node, "sets/"+this.namespace+"/"+setName); 
			String n_objectsString = infoString.substring(infoString.indexOf("=")+1, infoString.indexOf(":"));
			n_objects = Integer.parseInt(n_objectsString);
		}
		System.out.println(String.format("Total Master and Replica objects %d", n_objects));
		System.out.println(String.format("Total Master objects %d", (nodeCount > 1) ? n_objects/replicationCount : n_objects));
		
		return (nodeCount > 1) ? n_objects/replicationCount : n_objects;
	}
	
	
	protected <T> Iterable<T> findAllUsingQuery(Class<T> type, Filter filter){
		final Class<T> classType = type;
		Statement stmt = new Statement();
		stmt.setNamespace(this.namespace);
		stmt.setSetName(this.getSetName(type));
		if (filter != null)
			stmt.setFilters(filter);
		
		final RecordSet recordSet = this.client.query(null, stmt);
		Iterable<T> results = new Iterable<T> (){

			@Override
			public Iterator<T> iterator() {
				return new EntityIterator<T>(classType, converter, recordSet);
			}
			
		};
		return results;
	} 
	
	protected class EntityIterator<T> implements Iterator<T>, CloseableIterator<T>{
		
		private RecordSet recordSet;
		private Iterator<KeyRecord> recordSetIterator;
		private MappingAerospikeConverter converter;
		private Class<T> type;

		public EntityIterator(Class<T> type, MappingAerospikeConverter converter, RecordSet recordSet){
			this.converter = converter;
			this.type = type;
			this.recordSet = recordSet;
			this.recordSetIterator = recordSet.iterator();
		}

		@Override
		public boolean hasNext() {
			return this.recordSetIterator.hasNext();
		}

		@Override
		public T next() {
			KeyRecord keyRecord = this.recordSetIterator.next();
			AerospikeData data = AerospikeData.forRead(keyRecord.key, null);
			data.setRecord(keyRecord.record);
			return converter.read(type, data);
		}

		@Override
		public void close()  {
			recordSet.close();
			
		}
		
	}


}
