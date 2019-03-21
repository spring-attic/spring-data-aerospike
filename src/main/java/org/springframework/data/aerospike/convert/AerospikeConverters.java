/*
 * Copyright 2011-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.convert;

import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.client.Value.GeoJSONValue;

/**
 * Wrapper class to contain useful converters 
 * 
 * @author Peter Milne
 */
abstract class AerospikeConverters {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private AerospikeConverters() {}

	public static enum LongToValueConverter implements Converter<Long, Value> {
		INSTANCE;

		public Value convert(Long source) {
			return source == null ? null : Value.get(source);
		}
	}

	public static enum BinToLongConverter implements Converter<Bin, Long> {
		INSTANCE;

		public Long convert(Bin source) {
			return source.value.toLong();
		}
	}

	public static enum StringToValueConverter implements Converter<String, Value> {
		INSTANCE;

		public Value convert(String source) {
			return source == null ? null : Value.get(source);
		}
	}

	public static enum BinToStringConverter implements Converter<Bin, String> {
		INSTANCE;

		public String convert(Bin source) {
			return source.value.toString();
		}
	}

	public static enum ListToValueConverter implements Converter<List<?>, Value> {
		INSTANCE;

		public Value convert(List<?> source) {
			return source == null ? null : Value.get(source);
		}
	}

	public static enum MapToValueConverter implements Converter<Map<?, ?>, Value> {
		INSTANCE;

		public Value convert(Map<?, ?> source) {
			return source == null ? null : Value.get(source);
		}
	}

	public static enum BytesToValueConverter implements Converter<Byte[], Value> {
		INSTANCE;

		public Value convert(Byte[] source) {
			return source == null ? null : Value.get(source);
		}
	}
	
	public static enum StringToAerospikeGeoJSONValueConverter implements Converter<String, GeoJSONValue>{
		INSTANCE;
		
		/* (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJSONValue convert(String source) {
			return new GeoJSONValue(source);
		}
	}

}
