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
import java.util.Collection;
import java.util.List;

import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.convert.AerospikeData;
import org.springframework.data.keyvalue.core.AbstractKeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueTemplate;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

/**
 * An Aerospike-specific {@link KeyValueAdapter} to implement core sore interactions to be used by the
 * {@link KeyValueTemplate}.
 * 
 * @author Oliver Gierke
 */
public class AerospikeKeyValueAdapter extends AbstractKeyValueAdapter {

	private final AerospikeConverter converter;
	private final AerospikeClient client;

	private String namespace;
	private final WritePolicy writePolicy;

	/**
	 * Creates a new {@link AerospikeKeyValueAdapter} using the given {@link AerospikeClient} and
	 * {@link AerospikeConverter}.
	 * 
	 * @param client must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public AerospikeKeyValueAdapter(AerospikeClient client, AerospikeConverter converter) {

		this.client = client;
		this.converter = converter;
		this.writePolicy = new WritePolicy();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#put(java.io.Serializable, java.lang.Object, java.io.Serializable)
	 */
	@Override
	public Object put(Serializable id, Object item, Serializable keyspace) {

		AerospikeData data = AerospikeData.forWrite(namespace);
		converter.write(item, data);

		List<Bin> bins = data.getBins();

		client.put(writePolicy, data.getKey(), bins.toArray(new Bin[bins.size()]));

		return item;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#contains(java.io.Serializable, java.io.Serializable)
	 */
	@Override
	public boolean contains(Serializable id, Serializable keyspace) {

		Key key = new Key(namespace, keyspace.toString(), id.toString());

		return client.exists(null, key);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#get(java.io.Serializable, java.io.Serializable)
	 */
	@Override
	public Object get(Serializable id, Serializable keyspace) {

		Key key = new Key(namespace, keyspace.toString(), id.toString());

		Record record = client.get(null, key);

		return converter.read(Object.class, AerospikeData.forRead(key, record));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#delete(java.io.Serializable, java.io.Serializable)
	 */
	@Override
	public Object delete(Serializable id, Serializable keyspace) {

		Key key = new Key(namespace, keyspace.toString(), id.toString());

		Object object = get(id, keyspace);

		if (object != null) {
			client.delete(writePolicy, key);
		}

		return object;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#getAllOf(java.io.Serializable)
	 */
	@Override
	public Collection<?> getAllOf(Serializable keyspace) {

		Statement statement = new Statement();
		statement.setNamespace(namespace);
		statement.setSetName(keyspace.toString());

		List<Object> result = new ArrayList<Object>();
		RecordSet recordSet = client.query(null, statement);

		while (recordSet.next()) {

			AerospikeData data = AerospikeData.forRead(recordSet.getKey(), recordSet.getRecord());
			result.add(converter.read(Object.class, data));
		}

		throw new UnsupportedOperationException();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#deleteAllOf(java.io.Serializable)
	 */
	@Override
	public void deleteAllOf(Serializable keyspace) {
		throw new UnsupportedOperationException();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#clear()
	 */
	@Override
	public void clear() {}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {}

}
