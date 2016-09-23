package org.springframework.data.aerospike.convert;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;

/**
 * @author Mustafa Zidan
 */
@SuppressWarnings("rawtypes")
final class EnumToStringConverterFactory<E extends Enum> implements ConverterFactory< E, String> {

	@Override
	public <T extends String> Converter<E, T> getConverter(Class<T> targetType) {
		return new EnumToStringConverter<E, T>(targetType);
	}

	private class EnumToStringConverter<E, T> implements Converter<E, T> {

		@SuppressWarnings("unused")
		private Class<T> enumType;

		public EnumToStringConverter(Class<T> targetType) {
			this.enumType = targetType;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T convert(E source) {
			return (T) source.toString();
		}
	}
}
