/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.convert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.context.ApplicationContext;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * An implementation of {@link AerospikeConverter} to read domain objects from {@link AerospikeData} and write domain
 * objects into them.
 * 
 * @author Oliver Gierke
 */
public class MappingAerospikeConverter implements AerospikeConverter {

	private final AerospikeMappingContext mappingContext;
	private final ConversionService conversionService;
	private final EntityInstantiators entityInstantiators;
	private final TypeMapper<AerospikeData> typeMapper;

	protected ApplicationContext applicationContext;
	protected final SpelExpressionParser spelExpressionParser = new SpelExpressionParser();

	/**
	 * Creates a new {@link MappingAerospikeConverter}.
	 */
	public MappingAerospikeConverter() {
		this.mappingContext = new AerospikeMappingContext();
		this.conversionService = new DefaultConversionService();
		this.entityInstantiators = new EntityInstantiators();

		this.typeMapper = new DefaultTypeMapper<AerospikeData>(AerospikeTypeAliasAccessor.INSTANCE, mappingContext,
				Arrays.asList(SimpleTypeInformationMapper.INSTANCE));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getMappingContext()
	 */
	@Override
	public MappingContext<? extends AerospikePersistentEntity<?>, KeyValuePersistentProperty> getMappingContext() {
		return mappingContext;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getConversionService()
	 */
	@Override
	public ConversionService getConversionService() {
		return conversionService;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityReader#read(java.lang.Class, S)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <R> R read(Class<R> type, AerospikeData data) {
		TypeInformation<?> readType = typeMapper.readType(data, ClassTypeInformation.from(type));
		TypeInformation<?> typeToUse = type.isAssignableFrom(readType.getType()) ? readType : ClassTypeInformation
				.from(type);

		final AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(typeToUse);
		final RecordReadingPropertyValueProvider recordReadingPropertyValueProvider = new RecordReadingPropertyValueProvider(
				data.getRecord());

		EntityInstantiator instantiator = entityInstantiators.getInstantiatorFor(entity);
		Object instance = instantiator.createInstance(entity,
				new PersistentEntityParameterValueProvider<KeyValuePersistentProperty>(entity,
						recordReadingPropertyValueProvider, null));

		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(instance);

		entity.doWithProperties(new PropertyHandler<KeyValuePersistentProperty>() {

			@Override
			public void doWithPersistentProperty(KeyValuePersistentProperty persistentProperty) {

				PreferredConstructor<?, KeyValuePersistentProperty> constructor = entity.getPersistenceConstructor();

				if (constructor.isConstructorParameter(persistentProperty)) {
					return;
				}

				accessor.setProperty(persistentProperty,
						recordReadingPropertyValueProvider.getPropertyValue(persistentProperty));
			}
		});

		return (R) instance;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void write(Object source, final AerospikeData data) {

		final AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());
		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(source);
		final List<Bin> bins = new ArrayList<Bin>();

		entity.doWithProperties(new PropertyHandler<KeyValuePersistentProperty>() {

			@Override
			public void doWithPersistentProperty(KeyValuePersistentProperty property) {

				if (property.isIdProperty()) {

					data.setKey(new Key(data.getNamespace(), entity.getSetName(), accessor.getProperty(property).toString()));
					return;
				}

				bins.add(new Bin(property.getName(), accessor.getProperty(property)));
			}
		});
		typeMapper.writeType(entity.getTypeInformation(), data);
		data.add(bins);
	}

	/**
	 * A {@link PropertyValueProvider} to lookup values on the configured {@link Record}.
	 *
	 * @author Oliver Gierke
	 */
	private static class RecordReadingPropertyValueProvider implements PropertyValueProvider<KeyValuePersistentProperty> {

		private final Record record;

		/**
		 * Creates a new {@link RecordReadingPropertyValueProvider} for the given {@link Record}.
		 * 
		 * @param record must not be {@literal null}.
		 */
		public RecordReadingPropertyValueProvider(Record record) {
			this.record = record;
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mapping.model.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public <T> T getPropertyValue(KeyValuePersistentProperty property) {
			return (T) record.getValue(property.getName());
		}
	}

	private static enum AerospikeTypeAliasAccessor implements TypeAliasAccessor<AerospikeData> {

		INSTANCE;

		private static final String TYPE_BIN_NAME = "spring_class";

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.convert.TypeAliasAccessor#readAliasFrom(java.lang.Object)
		 */
		@Override
		public Object readAliasFrom(AerospikeData source) {
			return source.getRecord().getValue(TYPE_BIN_NAME);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.convert.TypeAliasAccessor#writeTypeTo(java.lang.Object, java.lang.Object)
		 */
		@Override
		public void writeTypeTo(AerospikeData sink, Object alias) {
			sink.add(new Bin(TYPE_BIN_NAME, alias.toString()));
		}
	}
}
