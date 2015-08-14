package org.springframework.data.aerospike.cache;


import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class AerospikeCache implements Cache{
	private static final String VALUE = "value";
	private final AerospikeClient client;
	private String namespace;
	private String set;
	private WritePolicy createOnly;
	
	public AerospikeCache(String namespace, String set, AerospikeClient client,
			long expiration){
		this.client = client;
		this.namespace = namespace;
		this.set = set;
		this.createOnly = new WritePolicy(client.writePolicyDefault);
		this.createOnly.recordExistsAction = RecordExistsAction.CREATE_ONLY;
	}
	
	private Key getKey(Object key){
		return new Key(namespace, set, key.toString());
	}
	private ValueWrapper toWrapper(Object value) {
		return (value != null ? new SimpleValueWrapper(value) : null);
	}
	
	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void evict(Object key) {
		this.client.delete(null, getKey(key));
		
	}

	@Override
	public ValueWrapper get(Object key) {
		Record record =  client.get(null, getKey(key));
		ValueWrapper vr = toWrapper(record.getValue(VALUE));
		return vr;
	}

	@Override
	public <T> T get(Object key, Class<T> type) {
		return (T) client.get(null, getKey(key));
	}

	@Override
	public String getName() {
		return this.namespace+":"+this.set;
	}

	@Override
	public Object getNativeCache() {
		return client;
	}

	@Override
	public void put(Object key, Object value) {
		client.put(null, getKey(key), new Bin(VALUE, value));
		
	}

	@Override
	public ValueWrapper putIfAbsent(Object key, Object value) {
		Record record = client.operate(this.createOnly, getKey(key), Operation.put(new Bin(VALUE, value)), Operation.get(VALUE));
		return toWrapper(record.getValue(VALUE));
	}

}
