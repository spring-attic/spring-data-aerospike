package org.springframework.data.aerospike.convert;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author Mustafa Zidan
 */
final class LocalDateTimeToStringConverter<T extends String> implements Converter<LocalDateTime, T> {

    public T convert(LocalDateTime source) {
        return (T) source.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

}