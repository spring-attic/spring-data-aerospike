/* Copyright 2012-2015 Aerospike, Inc.
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
/* Copyright 2012-2015 Aerospike, Inc.
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
package com.aerospike.helper.query;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.query.Filter;
/**
 * Generic Bin qualifier. It acts as a filter to exclude records that do not met this criteria.
 * The operations supported are 
 * <li>EQ - Equals</li>
 * <li>GT - Greater than</li>
 * <li>GTEQ - Greater than or equal to</li>
 * <li>LT - Less than</li>
 * <li>LTEQ - Less than or equal to</li>
 * <li>NOTEQ - Not equal</li>
 * <li>BETWEEN - Between two value (inclusive)</li>
 * <li>START_WITH - A string that starts with</li>
 * <li>ENDS_WITH - A string that ends with</li>
 * 
 * @author peter
 *
 */
public class Qualifier implements Map<String, Object>{
	private static final String FIELD = "field";
	private static final String VALUE2 = "value2";
	private static final String VALUE1 = "value1";
	private static final String OPERATION = "operation";
	protected Map<String, Object> internalMap;
	public enum FilterOperation {
		EQ, GT, GTEQ, LT, LTEQ, NOTEQ, BETWEEN, START_WITH, ENDS_WITH
	}

	public Qualifier() {
		super();
		internalMap = new HashMap<String, Object>();
	}
	public Qualifier(String field, FilterOperation operation, Value value1) {
		this();
		internalMap.put(FIELD, field);
		internalMap.put(OPERATION, operation);
		internalMap.put(VALUE1, value1);
	}
	public Qualifier(String field, FilterOperation operation, Value value1, Value value2) {
		this(field, operation, value1);
		internalMap.put(VALUE2, value2);
	}

	public FilterOperation getOperation(){
		return (FilterOperation) internalMap.get(OPERATION);
	}
	public String getField(){
		return (String) internalMap.get(FIELD);
	}
	public Value getValue1(){
		return (Value) internalMap.get(VALUE1);
	}
	public Value getValue2(){
		return (Value) internalMap.get(VALUE2);
	}

	@SuppressWarnings("deprecation")
	public Filter asFilter(){
		FilterOperation op = getOperation();
		switch (op) {
		case EQ:
			return Filter.equal(getField(), getValue1());
		case BETWEEN:
			return Filter.range(getField(), getValue1(), getValue2());
		default:
			return null;
		}
	}

	public String luaFilterString(){
		String value1 = luaValueString(getValue1());
		FilterOperation op = getOperation();
		switch (op) {
		case EQ:
			return String.format("%s == %s", luaFieldString(getField()),  value1);
		case NOTEQ:
			return String.format("%s ~= %s", luaFieldString(getField()), value1);
		case GT:
			return String.format("%s > %s", luaFieldString(getField()), value1);
		case GTEQ:
			return String.format("%s >= %s", luaFieldString(getField()), value1);
		case LT:
			return String.format("%s < %s", luaFieldString(getField()), value1);
		case LTEQ:
			return String.format("%s <= %s", luaFieldString(getField()), value1);
		case BETWEEN:
			String value2 = luaValueString(getValue2());
			String fieldString = luaFieldString(getField()); 
			return String.format("%s >= %s and %s <= %s  ", fieldString, value1, luaFieldString(getField()), value2);
		case START_WITH:
			return String.format("string.sub(%s,1,string.len(%s))==%s", luaFieldString(getField()), value1, value1);			
		case ENDS_WITH:
			return String.format("%s=='' or string.sub(%s,-string.len(%s))==%s", 
					value1,
					luaFieldString(getField()),
					value1,
					value1);			
		}
		return "";
	}
	
	protected String luaFieldString(String field){
		return String.format("rec['%s']", field);
	}

	protected String luaValueString(Value value){
		String res = null;
		int type = value.getType();
		switch (type) {
//		case ParticleType.LIST:
//			res = value.toString();
//			break;
//		case ParticleType.MAP:
//			res = value.toString();
//			break;
//		case ParticleType.DOUBLE:
//			res = value.toString();
//			break;
		case ParticleType.STRING:
			res = String.format("'%s'", value.toString());
			break;
		default:
			res = value.toString();
			break;
		}
		return res;
	}



	/*
	 * (non-Javadoc)
	 * @see java.util.Map#size()
	 */
	@Override
	public int size() {
		return internalMap.size();
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return internalMap.isEmpty();
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#containsKey(java.lang.Object)
	 */
	@Override
	public boolean containsKey(java.lang.Object key) {
		return internalMap.containsKey(key);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#containsValue(java.lang.Object)
	 */
	@Override
	public boolean containsValue(java.lang.Object value) {
		return internalMap.containsValue(value);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#get(java.lang.Object)
	 */
	@Override
	public Object get(java.lang.Object key) {
		return internalMap.get(key);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Object put(String key, Object value) {
		return internalMap.put(key, value);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#remove(java.lang.Object)
	 */
	@Override
	public Object remove(java.lang.Object key) {
		return internalMap.remove(key);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		internalMap.putAll(m);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#clear()
	 */
	@Override
	public void clear() {
		internalMap.clear();
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#keySet()
	 */
	@Override
	public Set<String> keySet() {
		return internalMap.keySet();
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#values()
	 */
	@Override
	public Collection<Object> values() {
		return internalMap.values();
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#entrySet()
	 */
	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		return internalMap.entrySet();
	}

	@Override
	public String toString() {
		String output = String.format("%s:%s:%s:%s",getField(), getOperation(), getValue1(), getValue2());
		return output;
	}
}
