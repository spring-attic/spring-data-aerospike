/* 
 * Copyright 2012-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.helper.collections;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.WritePolicy;

/**
 * This class provides a TimeSeries collection for a "top record"
 *
 * @author Peter Milne
 */
public class TimeSeries {
	/**
	 * the bin in the top record to hold the TimeSeries configuration
	 */
	public static final String tsKey = "TimeStamp";
	public static final String tsValue = "Value";

	public static final long DefaultBucketSize = 1000; // In milliseconds

	private static final String configBucketSize = "bucketSize";

	private static final String valueBin = "____TSvalue";
	private static final String topBin = "____TStop";
	private static final String tailBin = "____TStail";
	private AerospikeClient client;
	private WritePolicy policy;
	private Key key;
	private String binName;
	private long bucketSize; // Bucket size in milliseconds

	/**
	 * Private constructor for test only
	 */
	TimeSeries(Key topRecordKey, long bucketSize) {
		this.bucketSize = bucketSize;
		this.key = topRecordKey;
	}

	public TimeSeries(AerospikeClient client, WritePolicy policy, Key key, String binName) {
		super();
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = binName;
		config();
	}

	@SuppressWarnings("unchecked")
	private void config() {
		Map<String, Object> conf = null;
		Record record = this.client.get(this.policy, key, binName);
		if (record != null) {
			conf = (Map<String, Object>) record.getValue(binName);
		}
		if (conf == null) {
			conf = new HashMap<String, Object>();
			conf.put(configBucketSize, DefaultBucketSize);
			this.bucketSize = DefaultBucketSize;
			this.client.put(this.policy, this.key, new Bin(binName, Value.get(conf)));
		} else {
			this.bucketSize = (Long) conf.get(configBucketSize);
		}
	}

	public void add(long timeStamp, Value value) {
		Key subKey = formSubrecordKey(timeStamp);
		this.client.operate(this.policy, subKey, ListOperation.append(valueBin, Value.get(new Entry(timeStamp, value).toMap())));
	}

	public void add(List<Map<String, Object>> values) {
		for (Map<String, Object> entry : values) {
			if (entry.containsKey(tsKey) && entry.containsKey(tsValue)) {
				Key subKey = formSubrecordKey((Long) entry.get(tsKey));
				this.client.operate(this.policy, subKey, ListOperation.append(valueBin, Value.get(entry)));
			}
		}
	}

	@SuppressWarnings("unchecked")
	public Value find(long timeStamp) {
		Key subKey = formSubrecordKey(timeStamp);
		Record record = this.client.get(this.policy, subKey);
		if (record != null) {
			List<Value> values = (List<Value>) record.getList(valueBin);
			for (Value value : values) {
				// TODO do something clever here
			}
		}
		return null;
	}

	/**
	 * clear all elements from the TimeSeries associated with a Key
	 */
	public void clear() {
		Record record = this.client.get(null, key, tailBin, topBin);
		long tail = record.getLong(tailBin);
		long top = record.getLong(topBin);
		List<Key> subKeys = subrecordKeys(tail, top);
		for (Key key : subKeys) {
			this.client.delete(null, key);
		}
	}

	/**
	 * Destroy the TimeSeries associated with a Key
	 */
	public void destroy() {
		clear();
		this.client.operate(null, key, Operation.put(Bin.asNull(binName)));
	}

	/**
	 * creates a list of Keys in the time series
	 *
	 * @param lowTime
	 * @param highTime
	 * @return
	 */
	List<Key> subrecordKeys(long lowTime, long highTime) {
		List<Key> keys = new ArrayList<Key>();
		long lowBucketNumber = bucketNumber(lowTime);
		long highBucketNumber = bucketNumber(highTime);
		for (long index = lowBucketNumber; index <= highBucketNumber; index += this.bucketSize) {
			keys.add(formSubrecordKey(index));
		}
		return keys;
	}

	Key formSubrecordKey(long timeStamp) {
		String keyString = String.format("%s::%s", this.key.userKey.toString(), Long.toHexString(bucketNumber(timeStamp)));
		return new Key(this.key.namespace, this.key.setName, keyString);
	}

	long bucketNumber(long timeStamp) {
		long quantPart = timeStamp / bucketSize;
		long bucketNumber = quantPart * bucketSize;
		return bucketNumber;
	}

	public Key getKey() {
		return this.key;
	}

	public class Entry {
		private long timeStamp;
		private Value value;

		public Entry(long timeStamp, Value value) {
			this.timeStamp = timeStamp;
			this.value = value;
		}

		Map<String, Object> internalMap;

		public long getTimeStamp() {
			return this.timeStamp;
		}

		public Value getValue() {
			return this.value;
		}

		public Map<String, Object> toMap() {
			Map<String, Object> entry = new HashMap<String, Object>();
			entry.put(tsKey, timeStamp);
			entry.put(tsValue, value);
			return entry;
		}
	}

}
