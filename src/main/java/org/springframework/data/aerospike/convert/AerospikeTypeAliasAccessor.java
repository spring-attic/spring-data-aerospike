package org.springframework.data.aerospike.convert;

import org.springframework.data.convert.TypeAliasAccessor;

import java.util.Map;

public class AerospikeTypeAliasAccessor implements TypeAliasAccessor<Map<String, Object>> {

	private static final String TYPE_KEY = "@_class";

	@Override
	public Object readAliasFrom(Map<String, Object> source) {
		return source.get(TYPE_KEY);
	}

	@Override
	public void writeTypeTo(Map<String, Object> sink, Object alias) {
		sink.put(TYPE_KEY, alias);
	}
}