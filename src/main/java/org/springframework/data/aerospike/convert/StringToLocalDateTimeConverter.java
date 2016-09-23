package org.springframework.data.aerospike.convert;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.core.convert.converter.Converter;

/**
 * @author Mustafa Zidan
 */
final class StringToLocalDateTimeConverter<T extends LocalDateTime> implements Converter<String, T> {

	@SuppressWarnings("unchecked")
	public T convert(String source) {
		return (T) LocalDateTime.parse(source, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
	}
}