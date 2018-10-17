package org.springframework.data.aerospike.convert;

import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.mapping.Alias;

import java.util.Map;

public class AerospikeTypeAliasAccessor implements TypeAliasAccessor<Map<String, Object>> {

	private static final String TYPE_KEY = "@_class";
	private final String typeKey;

	public AerospikeTypeAliasAccessor(String typeKey) {
		this.typeKey = typeKey;
	}

	public AerospikeTypeAliasAccessor() {
		this.typeKey = TYPE_KEY;
	}

	@Override
	public Alias readAliasFrom(Map<String, Object> source) {
		return Alias.ofNullable(source.get(typeKey));
	}

	@Override
	public void writeTypeTo(Map<String, Object> sink, Object alias) {
		if (typeKey != null) {
			sink.put(typeKey, alias);
		}
	}
}