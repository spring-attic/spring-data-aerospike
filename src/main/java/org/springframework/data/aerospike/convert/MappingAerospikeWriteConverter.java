package org.springframework.data.aerospike.convert;

import com.aerospike.client.Key;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.convert.EntityWriter;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.springframework.data.aerospike.convert.AerospikeMetaData.USER_KEY;

public class MappingAerospikeWriteConverter implements EntityWriter<Object, AerospikeWriteData> {

	private final TypeMapper<Map<String, Object>> typeMapper;
	private final AerospikeMappingContext mappingContext;
	private final CustomConversions conversions;
	private final GenericConversionService conversionService;

	public MappingAerospikeWriteConverter(TypeMapper<Map<String, Object>> typeMapper,
										  AerospikeMappingContext mappingContext, CustomConversions conversions,
										  GenericConversionService conversionService) {
		this.typeMapper = typeMapper;
		this.mappingContext = mappingContext;
		this.conversions = conversions;
		this.conversionService = conversionService;
	}

	@Override
	public void write(Object source, final AerospikeWriteData data) {
		if (source == null) {
			return;
		}

		TypeInformation<?> type = ClassTypeInformation.from(source.getClass());
		AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());
		ConvertingPropertyAccessor accessor = new ConvertingPropertyAccessor(entity.getPropertyAccessor(source), conversionService);

		AerospikePersistentProperty idProperty = entity.getIdProperty();
		if (idProperty != null) {
			String id = accessor.getProperty(idProperty, String.class);
			Assert.notNull(id, "Id must not be null!");

			data.setKey(new Key(entity.getNamespace(), entity.getSetName(), id));
			data.addBin(USER_KEY, id);
		}

		data.setExpiration(entity.getExpiration());

		Map<String, Object> convertedProperties = convertProperties(type, entity, accessor);
		convertedProperties.entrySet().forEach(e -> data.addBin(e.getKey(), e.getValue()));
	}

	private Map<String, Object> convertProperties(TypeInformation<?> type, AerospikePersistentEntity<?> entity, ConvertingPropertyAccessor accessor) {
		Map<String, Object> target = new HashMap<>();
		typeMapper.writeType(type, target);
		entity.doWithProperties((PropertyHandler<AerospikePersistentProperty>) property -> {

			Object value = accessor.getProperty(property);
			if (property.isIdProperty() || !property.isWritable()) {
				return;
			}
			Object valueToWrite = getValueToWrite(value, property.getTypeInformation());
			target.put(property.getFieldName(), valueToWrite);
		});
		return target;
	}

	private Object getValueToWrite(Object value, TypeInformation<?> type) {
		if (value == null) {
			// TODO: should we write nulls to storage?
			return null;
		} else if (type == null || conversions.isSimpleType(value.getClass())) {
			return getSimpleValueToWrite(value);
		} else {
			return getNonSimpleValueToWrite(value, type);
		}
	}

	private Object getSimpleValueToWrite(Object value) {
		Class<?> customTarget = conversions.getCustomWriteTarget(value.getClass());
		if (customTarget != null) {
			return conversionService.convert(value, customTarget);
		}
		return value;
	}

	private Object getNonSimpleValueToWrite(Object value, TypeInformation<?> type) {
		TypeInformation<?> valueType = ClassTypeInformation.from(value.getClass());

		if (valueType.isCollectionLike()) {
			return convertCollection(asCollection(value), type);
		}

		if (valueType.isMap()) {
			return convertMap(asMap(value), type);
		}

		Class<?> basicTargetType = conversions.getCustomWriteTarget(value.getClass(), null);
		if (basicTargetType != null) {
			return conversionService.convert(value, basicTargetType);
		}

		return convertCustomType(value, valueType);
	}

	private List<Object> convertCollection(final Collection<?> source, final TypeInformation<?> type) {
		Assert.notNull(source, "Given collection must not be null!");
		Assert.notNull(type, "Given type must not be null!");

		TypeInformation<?> componentType = type.getComponentType();

		return source.stream().map(element -> getValueToWrite(element, componentType)).collect(Collectors.toList());
	}

	private Map<String, Object> convertMap(final Map<Object, Object> source, final TypeInformation<?> type) {
		Assert.notNull(source, "Given map must not be null!");
		Assert.notNull(type, "Given type must not be null!");

		return source.entrySet().stream().collect(HashMap::new, (m, e) -> {
			Object key = e.getKey();
			Object value = e.getValue();
			if (!conversions.isSimpleType(key.getClass())) {
				throw new MappingException("Cannot use a complex object as a key value.");
			}
			String simpleKey = key.toString();
			Object convertedValue = getValueToWrite(value, type.getMapValueType());
			m.put(simpleKey, convertedValue);
		}, HashMap::putAll);
	}

	private Map<String, Object> convertCustomType(Object source, TypeInformation<?> type) {
		Assert.notNull(source, "Given map must not be null!");
		Assert.notNull(type, "Given type must not be null!");

		AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());
		ConvertingPropertyAccessor accessor = new ConvertingPropertyAccessor(entity.getPropertyAccessor(source), conversionService);

		return convertProperties(type, entity, accessor);
	}

	@SuppressWarnings("unchecked")
	private Map<Object, Object> asMap(Object value) {
		return (Map<Object, Object>) value;
	}

	private static Collection<?> asCollection(final Object source) {
		if (source instanceof Collection) {
			return (Collection<?>) source;
		}
		return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
	}
}
