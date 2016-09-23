package org.springframework.data.aerospike.convert;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.core.convert.converter.Converter;

/**
 * @author Mustafa Zidan
 */
final class LocalDateTimeToStringConverter<T extends String> implements Converter<LocalDateTime, T> {

	@SuppressWarnings("unchecked")
	public T convert(LocalDateTime source) {
		return (T) source.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
	}

}