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
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.WritePolicy;

/**
 * An implementation of LargeList using standard KV operations.
 * <p>
 * <STRONG>Not all operations are implemented, methods that invoke UDFs for filtering are not implemented and will throw a NotImplementedException if called</STRONG>
 * <p>
 * This code fragment shows how to create a LargeList.  An existing AerospikeClient is passed to the constructor, along wit an optional WritePolicy, the Key of the top record and the name of the large list Bin.
 * <pre>
 * {@code
 * AerospikeClient client = new AerospikeClient(clientPolicy, "127.0.0.1", 3000);
 * com.aerospike.helper.collections.LargeList ll = new com.aerospike.helper.collections.LargeList (client, null, key, "100-int");
 * }
 * </pre>
 * <p>
 *
 * @author Peter Milne
 */
public class LargeList {
	public static final String ListElementBinName = "__ListElement";

	private AerospikeClient client;
	private WritePolicy policy;
	private Key key;
	private Value binName;
	private String binNameString;

	/**
	 * Initialize large list operator.
	 * <p>
	 *
	 * @param client  client
	 * @param policy  generic configuration parameters, pass in null for defaults
	 * @param key	 unique record identifier of the top record
	 * @param binName bin name
	 */

	public LargeList(AerospikeClient client, WritePolicy policy, Key key, String binName) {
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = Value.get(binName);
		this.binNameString = this.binName.toString();
	}

	@SuppressWarnings("rawtypes")
	private Key makeSubKey(Value value) {
		Key subKey;
		String valueString;
		if (value instanceof Value.MapValue) {
			Map map = (Map) value.getObject();
			valueString = map.get("key").toString();
		} else {
			valueString = value.toString();
		}
		
		String subKeyString = String.format("%s::%s", this.key.userKey.toString(), valueString);
		subKey = new Key(this.key.namespace, this.key.setName, subKeyString);
		return subKey;
	}

	private Key[] makeSubKeys(List<Value> values) {
		Key[] keys = new Key[values.size()];
		int index = 0;
		for (Value value : values) {
			keys[index] = makeSubKey(value);
			index++;
		}
		return keys;
	}

	@SuppressWarnings("unchecked")
	private Key[] getElementKeys() {
		Key[] keys = null;
		Record topRecord = client.get(this.policy, this.key, this.binNameString);
		if (topRecord != null) {
			List<byte[]> digestList = (List<byte[]>) topRecord.getList(this.binNameString);
			if (digestList != null) {
				keys = new Key[digestList.size()];
				int index = 0;
				for (byte[] digest : digestList) {
					Key subKey = new Key(this.key.namespace, digest, null, null);
					keys[index] = subKey;
					index++;
				}
			}
		}
		return keys;
	}

	@SuppressWarnings("rawtypes")
	private boolean filterBinByRange(Record record, String bin, Value low, Value high) {
		if (record == null)
			return false;
		Object value = record.getValue(bin);
		if (value instanceof List) {
			//TODO
			return false;
		} else if (value instanceof Map) {
			Map dict = (Map) value;
			Object keyValue = dict.get("key");
			if (keyValue == null)
				return false;
			return filterRange(keyValue, low, high);
		} else {
			return filterRange(value, low, high);
		}
	}

	private boolean filterRange(Object value, Value low, Value high) {
		if (value instanceof Long) {
			return (((Long) low.getObject()) <= (Long) value) && (((Long) high.getObject()) >= (Long) value);
		} else if (value instanceof String) {
			return (((String) low.getObject()).compareTo((String) value) <= 0) && (((String) high.getObject()).compareTo((String) value) >= 0);
		} else if (value instanceof Double) {
			return (((Double) low.getObject()) <= (Double) value) && (((Double) high.getObject()) >= (Double) value);
		} else {
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	private List<byte[]> getDigestList() {
		Record topRecord = client.get(this.policy, this.key, this.binNameString);
		if (topRecord == null)
			return new ArrayList<byte[]>();
		List<byte[]> digestList = (List<byte[]>) topRecord.getValue(this.binNameString);
		if (digestList == null)
			return new ArrayList<byte[]>();
		return digestList;
	}

	private List<Record> fetchSubRecords(Key[] subKeys) {
		List<Record> results = new ArrayList<Record>();
		Record[] records = client.get(null, subKeys);
		for (Record record : records) {
			if (record != null)
				results.add(record);
		}
		return results;
	}

	/**
	 * Add value to list. Fail if value's key exists and list is configured for unique keys.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 * <p>
	 *
	 * @param value value to add
	 */
	public void add(Value value) {
		Key subKey = makeSubKey(value);

		client.put(this.policy, subKey, new Bin(ListElementBinName, value));

		// add the digest of the subKey to the CDT List in the Customer record
		client.operate(this.policy, this.key, ListOperation.append(this.binNameString, Value.get(subKey.digest)));
	}

	/**
	 * Add values to the list.  Fail if a value's key exists and list is configured for unique keys.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 * <p>
	 *
	 * @param items values to add
	 */
	public void add(List<Value> items) {
		for (Value Value : items) {
			this.add(Value);
		}
	}

	/**
	 * Add values to list.  Fail if a value's key exists and list is configured for unique keys.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 *
	 * @param items values to add
	 */
	public void add(Value... items) {
		for (Value Value : items) {
			this.add(Value);
		}
	}

	/**
	 * Update value in list if key exists.  Add value to list if key does not exist.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 *
	 * @param value value to update
	 */
	public void update(Value value) {
		if (size() == 0) {
			add(value);
		} else {
			Key subKey = makeSubKey(value);
			client.put(this.policy, subKey, new Bin(ListElementBinName, value));
		}
	}

	/**
	 * Update/Add each value in array depending if key exists or not.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 *
	 * @param values An array of values to update
	 */
	public void update(Value... values) {
		for (Value value : values) {
			this.update(value);
		}
	}

	/**
	 * Update/Add each value in values list depending if key exists or not.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 *
	 * @param values A list of values to update
	 */
	public void update(List<?> values) {
		for (Object value : values) {
			this.update(Value.get(value));
		}
	}

	/**
	 * Delete value from list.
	 *
	 * @param value The value to value to delete
	 */
	public void remove(Value value) {
		Key subKey = makeSubKey(value);
		List<byte[]> digestList = getDigestList();
		int index = digestList.indexOf(subKey.digest);
		client.delete(this.policy, subKey);
		client.operate(this.policy, this.key, ListOperation.remove(this.binNameString, index));
	}

	/**
	 * Delete values from list.
	 *
	 * @param values A list of values to delete
	 */
	public void remove(List<Value> values) {
		Key[] keys = makeSubKeys(values);
		List<byte[]> digestList = getDigestList();

		//		int startIndex = digestList.IndexOf (subKey.digest);
		//		int count = values.Count;
		//		foreach (Key key in keys){
		//
		//			client.Delete (this.policy, key);
		//		}
		//		client.Operate(this.policy, this.key, ListOperation.Remove(this.binNameString, startIndex, count));

		for (Key key : keys) {
			client.delete(this.policy, key);
			digestList.remove(key.digest);
		}

		client.put(this.policy, this.key, new Bin(this.binNameString, digestList));
	}

	/**
	 * Delete values from list between range.
	 * <p>
	 *
	 * @param begin low value of the range (inclusive)
	 * @param end   high value of the range (inclusive)
	 * @return count of entries removed
	 */
	public int remove(Value begin, Value end) {
		List<byte[]> digestList = getDigestList();
		Key beginKey = makeSubKey(begin);
		Key endKey = makeSubKey(end);
		int start = digestList.indexOf(beginKey.digest);
		int stop = digestList.indexOf(endKey.digest);
		int count = stop - start + 1;
		;
		for (int i = start; i < stop; i++) {
			Key subKey = new Key(this.key.namespace, (byte[]) digestList.get(i), null, null);
			client.delete(this.policy, subKey);
		}
		client.operate(this.policy, this.key, ListOperation.removeRange(this.binNameString, start, count));
		return count;
	}

	/**
	 * Does key value exist?
	 * <p>
	 *
	 * @param keyValue key value to lookup
	 * @return true if value exists
	 */
	public boolean exists(Value keyValue) {
		Key subKey = makeSubKey(keyValue);
		return client.exists(this.policy, subKey);
	}

	/**
	 * Do key values exist?  Return list of results in one batch call.
	 * <p>
	 *
	 * @param keyValues key values to lookup
	 * @return list of booleans indicating which elements exist
	 */
	public List<Boolean> exists(List<Value> keyValues) throws AerospikeException {
		List<Boolean> target = new ArrayList<Boolean>();
		for (Object value : keyValues) {
			target.add(exists(Value.get(value)));
		}
		return target;
	}

	/**
	 * Select values from list.
	 *
	 * @param value value to select
	 * @return list of entries selected
	 */
	@SuppressWarnings("serial")
	public List<?> find(Value value) throws AerospikeException {
		Key subKey = makeSubKey(value);
		Record record = client.get(this.policy, subKey, ListElementBinName);
		if (record != null) {
			final Object result = record.getValue(ListElementBinName);
			return new ArrayList<Object>() {{
				add(result);
			}};
		} else {
			return null;
		}
	}

	private List<?> get(List<byte[]> digestList, int start, int stop) {
		List<Object> results = new ArrayList<Object>();

		for (int i = start; i < stop; i++) {
			Key subKey = new Key(this.key.namespace, (byte[]) digestList.get(i), null, null);
			Record record = client.get(this.policy, subKey, ListElementBinName);
			Object result = record.getValue(ListElementBinName);
			results.add(result);
		}
		return results;
	}

	/**
	 * Select values from the begin key up to a maximum count.
	 * <p>
	 *
	 * @param begin start value (inclusive)
	 * @param count maximum number of values to return
	 * @return list of entries selected
	 */
	public List<?> findFrom(Value begin, int count) throws AerospikeException {
		List<byte[]> digestList = getDigestList();
		Key beginKey = makeSubKey(begin);
		int start = digestList.indexOf(beginKey.digest);
		int stop = start + count;
		return get(digestList, start, stop);
	}

	/**
	 * Select a range of values from the large list.
	 * <p>
	 *
	 * @param begin low value of the range (inclusive)
	 * @param end   high value of the range (inclusive)
	 * @return list of entries selected
	 */
	public List<?> range(Value begin, Value end) {
		List<Object> results = new ArrayList<Object>();
		Key[] elementKeys = getElementKeys();
		if (elementKeys != null && elementKeys.length > 0) {
			List<Record> records = fetchSubRecords(elementKeys);
			for (Record record : records) {
				if (record != null && filterBinByRange(record, ListElementBinName, begin, end)) {
					results.add(record.getValue(ListElementBinName));
				}
			}
		}
		return results;
	}

	/**
	 * Select a range of values from the large list.
	 * <p>
	 * <STRONG>THIS METHOD IS NOT IMPLEMENTED - DO NOT USE</STRONG>
	 * <p>
	 *
	 * @param begin low value of the range (inclusive)
	 * @param end   high value of the range (inclusive)
	 * @param count maximum number of values to return, pass in zero to obtain all values within range
	 * @return list of entries selected
	 */
	@Deprecated
	public List<?> range(Value begin, Value end, int count) throws AerospikeException {
		throw new NotImplementedException();
	}

	/**
	 * Select a range of values from the large list, then apply a Lua filter.
	 * <p>
	 * <STRONG>THIS METHOD IS NOT IMPLEMENTED - DO NOT USE</STRONG>
	 * <p>
	 *
	 * @param begin		low value of the range (inclusive)
	 * @param end		  high value of the range (inclusive)
	 * @param filterModule Lua module name which contains filter function
	 * @param filterName   Lua function name which applies filter to returned list
	 * @param filterArgs   arguments to Lua function name
	 * @return list of entries selected
	 */
	@Deprecated
	public List<?> range(Value begin, Value end, String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		throw new NotImplementedException();
	}

	/**
	 * Select a range of values from the large list, then apply a lua filter.
	 * <p>
	 * <STRONG>THIS METHOD IS NOT IMPLEMENTED - DO NOT USE</STRONG>
	 * <p>
	 *
	 * @param begin		low value of the range (inclusive)
	 * @param end		  high value of the range (inclusive)
	 * @param count		maximum number of values to return after applying lua filter. Pass in zero to obtain all values within range.
	 * @param filterModule lua module name which contains filter function
	 * @param filterName   lua function name which applies filter to returned list
	 * @param filterArgs   arguments to lua function name
	 * @return list of entries selected
	 */
	@Deprecated
	public List<?> range(Value begin, Value end, int count, String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		throw new NotImplementedException();
	}

	public List<?> scan() {
		List<Object> results = new ArrayList<Object>();
		Key[] elementKeys = getElementKeys();
		if (elementKeys != null && elementKeys.length > 0) {
			List<Record> records = fetchSubRecords(elementKeys);
			for (Record record : records) {
				if (record != null)
					results.add(record.getValue(ListElementBinName));
			}
		}
		return results;
	}

	/**
	 * Select values from list and apply specified Lua filter.
	 * <p>
	 * <STRONG>THIS METHOD IS NOT IMPLEMENTED - DO NOT USE</STRONG>
	 * <p>
	 *
	 * @param filterModule Lua module name which contains filter function
	 * @param filterName   Lua function name which applies filter to returned list
	 * @param filterArgs   arguments to Lua function name
	 * @return list of entries selected
	 */
	@Deprecated
	public List<?> filter(String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		throw new NotImplementedException();
	}

	/**
	 * Delete bin containing the list.
	 */
	public void destroy() {
		List<byte[]> digestList = getDigestList();

		client.put(this.policy, this.key, Bin.asNull(this.binNameString));

		for (byte[] digest : digestList) {
			Key subKey = new Key(this.key.namespace, digest, null, null);
			client.delete(this.policy, subKey);
		}
	}

	/**
	 * Return size of list.
	 *
	 * @return size of list.
	 */
	public int size() {
		Record record = client.operate(this.policy, this.key, ListOperation.size(this.binNameString));
		if (record != null) {
			return record.getInt(this.binNameString);
		}
		return 0;
	}

	/**
	 * Return map of list configuration parameters.
	 * <p>
	 * <STRONG>THIS METHOD IS NOT IMPLEMENTED - DO NOT USE</STRONG>
	 * <p>
	 *
	 * @return null, deprecated
	 */
	@Deprecated
	public Map<?, ?> getConfig() {
		throw new NotImplementedException();
	}

	/**
	 * Set LDT page size.
	 * <p>
	 * <STRONG>THIS METHOD IS NOT IMPLEMENTED - DO NOT USE</STRONG>
	 * <p>
	 *
	 * @param pageSize Size of the LargeList memory page
	 */
	@Deprecated
	public void setPageSize(int pageSize) {
		throw new NotImplementedException();
	}

}

