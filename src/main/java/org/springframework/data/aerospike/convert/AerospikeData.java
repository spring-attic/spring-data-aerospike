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
package org.springframework.data.aerospike.convert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Value object to carry data to be read and to written in object conversion.
 * 
 * @author Oliver Gierke
 */
public class AerospikeData {

	private Key key;
	private final Record record;

	private final String namespace;
	private final List<Bin> bins;

	private AerospikeData(Key key, Record record, String namespace, List<Bin> bins) {

		this.key = key;
		this.record = record;
		this.namespace = namespace;
		this.bins = bins;
	}

	public static AerospikeData forRead(Key key, Record record) {
		return new AerospikeData(key, record, null, Collections.<Bin> emptyList());
	}

	public static AerospikeData forWrite(String namespace) {
		return new AerospikeData(null, null, namespace, new ArrayList<Bin>());
	}

	/**
	 * @return the key
	 */
	public Key getKey() {
		return key;
	}

	/**
	 * @param key the key to set
	 */
	public void setKey(Key key) {
		this.key = key;
	}

	/**
	 * @return the record
	 */
	public Record getRecord() {
		return record;
	}

	/**
	 * @return the namespace
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * @return the bins
	 */
	public List<Bin> getBins() {
		return bins;
	}

	public void add(List<Bin> bins) {
		this.bins.addAll(bins);
	}
}
