package org.springframework.data.aerospike.convert;

import com.aerospike.client.Key;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * Value object to carry data to be read in object conversion.
 * @author Oliver Gierke
 * @author Anastasiia Smirnova
 */
public class AerospikeReadData {

	private Key key;
	private final Map<String, Object> record;

	private AerospikeReadData(Key key, Map<String, Object> record) {
		this.key = key;
		this.record = record;
	}

	public static AerospikeReadData forRead(Key key, Map<String, Object> record) {
		Assert.notNull(record, "Record should not be null");
		Assert.notNull(key, "Key should not be null");
		return new AerospikeReadData(key, record);
	}

	public Map<String, Object> getRecord() {
		return record;
	}

	public Key getKey() {
		return key;
	}

	public Object getValue(String key) {
		return record.get(key);
	}
}
