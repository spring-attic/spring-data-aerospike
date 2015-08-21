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

import java.io.ObjectInputStream.GetField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.ApplicationContext;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikeMetadataBin;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.CachingAerospikePersistentProperty;
import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.Value.ListValue;
import com.aerospike.client.Value.MapValue;

/**
 * An implementation of {@link AerospikeConverter} to read domain objects from {@link AerospikeData} and write domain
 * objects into them.
 * 
 * @author Oliver Gierke
 */
public class MappingAerospikeConverter implements AerospikeConverter {

	private final AerospikeMappingContext mappingContext;
	private final SimpleTypeHolder simpleTypeHolder;
	private final ConversionService conversionService;
	private final EntityInstantiators entityInstantiators;
	private final TypeMapper<AerospikeData> typeMapper;
	public static final String SPRING_ID_BIN = "SpringID";

	protected ApplicationContext applicationContext;
	protected final SpelExpressionParser spelExpressionParser = new SpelExpressionParser();

	/**
	 * Creates a new {@link MappingAerospikeConverter}.
	 */
	public MappingAerospikeConverter() {
		this.mappingContext = new AerospikeMappingContext();
		DefaultConversionService defaultConversionService =  new DefaultConversionService();
		defaultConversionService.addConverter(new LongToBoolean());
		this.conversionService = defaultConversionService;
		this.entityInstantiators = new EntityInstantiators();
		this.simpleTypeHolder = new SimpleTypeHolder();

		this.typeMapper = new DefaultTypeMapper<AerospikeData>(AerospikeTypeAliasAccessor.INSTANCE, mappingContext,	Arrays.asList(SimpleTypeInformationMapper.INSTANCE));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getMappingContext()
	 */
	@Override
	public MappingContext<? extends AerospikePersistentEntity<?>, AerospikePersistentProperty> getMappingContext() {
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
	public <R> R read(Class<R> type, final AerospikeData data) {
		TypeInformation<?> readType = typeMapper.readType(data, ClassTypeInformation.from(type));
		TypeInformation<?> typeToUse = type.isAssignableFrom(readType.getType()) ? readType : ClassTypeInformation
				.from(type);

		final AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(typeToUse);
		final RecordReadingPropertyValueProvider recordReadingPropertyValueProvider = new RecordReadingPropertyValueProvider(data.getRecord(),getConversionService(),simpleTypeHolder);

		EntityInstantiator instantiator = entityInstantiators.getInstantiatorFor(entity);
		Object instance = instantiator.createInstance(entity,new PersistentEntityParameterValueProvider<AerospikePersistentProperty>(entity,recordReadingPropertyValueProvider, null));
		if (data.getRecord() != null) {

			final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(instance);

			entity.doWithProperties(new PropertyHandler<AerospikePersistentProperty>() {

				@Override
				public void doWithPersistentProperty(AerospikePersistentProperty persistentProperty) {
					PreferredConstructor<?, AerospikePersistentProperty> constructor = entity.getPersistenceConstructor();
					Record record = data.getRecord();
					if(record==null)return;
					Object propertyObject =  record.getValue(((CachingAerospikePersistentProperty)persistentProperty).getFieldName());
					if(propertyObject instanceof HashMap<?, ?> && ((Map) propertyObject).containsKey(MappingAerospikeConverter.SPRING_ID_BIN)){
						AerospikeData aerospikeData = AerospikeData.convertToAerospikeData((Map) propertyObject);
						if(aerospikeData==null)return;
						
						accessor.setProperty(persistentProperty,read(persistentProperty.getType(), aerospikeData));
						
						return;
						
					}
					
					if (constructor.isConstructorParameter(persistentProperty)) {
						return;
					}

					if (persistentProperty.isIdProperty()) {
						Object value = recordReadingPropertyValueProvider.getPropertyValue(persistentProperty,data.getSpringId());
						if (value != null) {
							accessor.setProperty(persistentProperty,value);
						}
						return;
					}

					Object value = recordReadingPropertyValueProvider.getPropertyValue(persistentProperty);
					if (value != null) {
						accessor.setProperty(persistentProperty,value);
					}
				}
			});
			
			
		} else {
			instance = null;
		}
		return (R) instance;
	}
	public <R> R read(Object instance, final AerospikeData data) {
		Class<R> type = (Class<R>) instance.getClass(); 
		TypeInformation<?> readType = typeMapper.readType(data, ClassTypeInformation.from(type));
		TypeInformation<?> typeToUse = type.isAssignableFrom(readType.getType()) ? readType : ClassTypeInformation
				.from(type);

		final AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(typeToUse);
		final RecordReadingPropertyValueProvider recordReadingPropertyValueProvider = new RecordReadingPropertyValueProvider(data.getRecord(),getConversionService(),simpleTypeHolder);

		if (data.getRecord() != null) {

			final PersistentPropertyAccessor accessor = entity
					.getPropertyAccessor(instance);

			entity.doWithProperties(new PropertyHandler<AerospikePersistentProperty>() {

				@Override
				public void doWithPersistentProperty(
						AerospikePersistentProperty persistentProperty) {
					PreferredConstructor<?, AerospikePersistentProperty> constructor = entity.getPersistenceConstructor();

					if (constructor.isConstructorParameter(persistentProperty)) {
						return;
					}

					if (persistentProperty.isIdProperty()) {
						Object value = recordReadingPropertyValueProvider.getPropertyValue(persistentProperty,data.getSpringId());
						if (value != null) {
							accessor.setProperty(persistentProperty,value);
						}
						return;
					}

					Object value = recordReadingPropertyValueProvider.getPropertyValue(persistentProperty);
					if (value != null) {
						accessor.setProperty(persistentProperty,value);
					}
				}
			});
			
			
		} else {
			instance = null;
		}
		return (R) instance;
	}

	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void write(Object source, final AerospikeData data) {
		
		if (null == source) {
			return;
		}
		final List<Bin> bins = new ArrayList<Bin>();
		Class<?> entityType = source.getClass();
		TypeInformation<? extends Object> type = ClassTypeInformation.from(entityType);
		
		writeInternal(source, data, type, bins);

//		final AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());
//		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(source);
//		final List<Bin> bins = new ArrayList<Bin>();
//		
//
//		entity.doWithProperties(new PropertyHandler<AerospikePersistentProperty>() {
//
//			@Override
//			public void doWithPersistentProperty(AerospikePersistentProperty property) {
//
//				if (property.isIdProperty()) {
//
//					data.setID(accessor.getProperty(property)!=null?accessor.getProperty(property).toString():null);
//					data.addMetaDataItem(SPRING_ID_BIN, accessor.getProperty(property));
//					//bins.add(new Bin(SPRING_ID_BIN,accessor.getProperty(property)));
//					return;
//				}
//				bins.add(new Bin(((CachingAerospikePersistentProperty) property).getFieldName(), accessor.getProperty(property)));
//			}
//		});
		
		
		data.add(bins);
		data.addMetaDataToBin();
		
		
	}

	/**
	 * @param target
	 * @param data
	 * @param type
	 * @param bins 
	 */
	protected void writeInternal(final Object obj, final AerospikeData data, final TypeInformation<?> type, final List<Bin> bins) {
		
		if (null == obj) {
			return;
		}
		
		Class<?> entityType = obj.getClass();
		
		if (Map.class.isAssignableFrom(entityType)) {
			//writeMapInternal((Map<Object, Object>) obj, data, ClassTypeInformation.MAP,bins);
			return;
		}
		
		if (Collection.class.isAssignableFrom(entityType)) {
			//writeCollectionInternal((Collection<?>) obj, ClassTypeInformation.LIST, data,bins);
			return;
		}
		
		AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(entityType);
		
		writeInternal(obj,data,entity,bins);
		
		typeMapper.writeType(entity.getTypeInformation(), data);
		if(data.getSetName()==null){
			data.setSetName(entity.getSetName());
		}		


		
	}

	/**
	 * @param obj
	 * @param data
	 * @param entity
	 */
	protected void writeInternal(Object obj, final AerospikeData data, AerospikePersistentEntity<?> entity,final List<Bin> bins) {
		if (obj == null) {
			return;
		}

		if (null == entity) {
			throw new MappingException("No mapping metadata found for entity of type " + obj.getClass().getName());
		}
		
		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(obj);
		final CachingAerospikePersistentProperty idProperty = (CachingAerospikePersistentProperty) entity.getIdProperty();
		
		if(idProperty!=null){
			Object id = accessor.getProperty(idProperty);
			data.setID(id!=null?id.toString():null);
			data.addMetaDataItem(SPRING_ID_BIN, id);
			data.addMetaDataItem(idProperty.getFieldName(), idProperty.getType());
			bins.add(new Bin(idProperty.getFieldName(), accessor.getProperty(idProperty)));
			
		}
		
		entity.doWithProperties(new PropertyHandler<AerospikePersistentProperty>() {

			@Override
					public void doWithPersistentProperty(AerospikePersistentProperty persistentProperty) {
						if (persistentProperty.equals(idProperty)|| !persistentProperty.isWritable()) {
							return;
						}

						Object propertyObj = accessor.getProperty(persistentProperty);

							if (propertyObj==null || simpleTypeHolder.isSimpleType(propertyObj.getClass())) {
								writeSimpleInternal(propertyObj, data, persistentProperty, accessor, bins);
							}
							else {
								writePropertyInternal(propertyObj, data, persistentProperty, accessor, bins);
								
							}
					}

				});
		
	}

	/**
	 * @param propertyObj
	 * @param data
	 * @param persistentProperty
	 * @param accessor
	 * @param bins
	 */
	protected void writePropertyInternal(Object propertyObj, AerospikeData data,AerospikePersistentProperty persistentProperty,	PersistentPropertyAccessor accessor, final List<Bin> bins) {
		if (propertyObj == null) {
			return;
		}
		
		TypeInformation<?> valueType = ClassTypeInformation.from(propertyObj.getClass());
		//TypeInformation<?> type = persistentProperty.getTypeInformation();
		String fieldName = ((CachingAerospikePersistentProperty) persistentProperty).getFieldName();
		data.addMetaDataItem(fieldName, valueType.getType());
		
		
		if (valueType.isCollectionLike()) {
			List<?> collection = asList(accessor.getProperty(persistentProperty));
			AerospikeData collectionData = AerospikeData.forWrite(data.getNamespace());
			List propertyList = new ArrayList();
			writeCollectionInternal(collection, valueType,  propertyList);
			Bin collectionBin = new Bin(fieldName, propertyList);
			bins.add(collectionBin);
		} else if (valueType.isMap()) {
			data.addMetaDataItem(fieldName, propertyObj.getClass());
			Value value = new MapValue((Map<?, ?>) accessor.getProperty(persistentProperty));
			bins.add(new Bin(fieldName, value));
		} else {
			
			AerospikePersistentEntity<?> childEntity = mappingContext.getPersistentEntity(propertyObj.getClass());
			AerospikeData childData = AerospikeData.forWrite(data.getNamespace());
			final List<Bin> childBins = new ArrayList<Bin>();
			writeInternal(propertyObj,childData,childEntity,childBins);
			
			typeMapper.writeType(childEntity.getTypeInformation(), childData);
			if(data.getSetName()==null){
				data.setSetName(childEntity.getSetName());
			}
			childData.add(childBins);
			childData.addMetaDataToBin();
			bins.add(new Bin(fieldName, AerospikeData.convertToMap(childData,this.simpleTypeHolder))); //works but does not finnish
			
			
			
		}

		
	}
	
	/**
	 * @param property
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static List<?> asList(Object source) {
		
		if (source instanceof Collection) {
			return  new ArrayList((Collection<?>)source);
		}
		return null;
	}

	/**
	 * Returns given object as {@link Collection}. Will return the {@link Collection} as is if the source is a
	 * {@link Collection} already, will convert an array into a {@link Collection} or simply create a single element
	 * collection for everything else.
	 * 
	 * @param source
	 * @return
	 */
	private static Collection<?> asCollection(Object source) {

		if (source instanceof Collection) {
			return (Collection<?>) source;
		}

		return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
	}

	/**
	 * @param propertyObj
	 * @param data
	 * @param persistentProperty
	 * @param bins 
	 * @param accessor 
	 */
	protected void writeSimpleInternal(Object propertyObj, AerospikeData data,	AerospikePersistentProperty persistentProperty, PersistentPropertyAccessor accessor, List<Bin> bins) {
		String fieldName = ((CachingAerospikePersistentProperty) persistentProperty).getFieldName();		
		data.addMetaDataItem(fieldName, persistentProperty.getType() );
		bins.add(new Bin(fieldName, accessor.getProperty(persistentProperty)));
		
	}

	/**
	 * @param <T>
	 * @param obj
	 * @param list
	 * @param data
	 * @param bins 
	 */
	protected <T> void writeCollectionInternal(Collection<?> collection,   TypeInformation<?> type,  List<T> propertyList) {

		TypeInformation<?> componentType = type == null ? null : type.getComponentType();
		Map map = null;
		
	
		for (Object element : collection) {
			if (element == null) {
				continue;
			}
			int count = 0;
			
			Class<?> elementType = element == null ? null : element.getClass();
			AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(elementType);
			
			if (elementType == null || simpleTypeHolder.isSimpleType(elementType)) {
				propertyList.add((T) element);
			} else if (element instanceof Collection || elementType.isArray()) {
				//bins.add(writeCollectionInternal(asCollection(element), componentType, new BasicDBList()));
			} else {
				AerospikeData childData = AerospikeData.forWrite(entity.getSetName());
				final List<Bin> childBins = new ArrayList<Bin>();
				writeInternal(element,childData,entity,childBins);
				
				typeMapper.writeType(entity.getTypeInformation(), childData);
				childData.add(childBins);
				childData.addMetaDataToBin();
				map = AerospikeData.convertToMap(childData,simpleTypeHolder);
				propertyList.add((T) map);
				//works but does not finnish
				
			}
			

		}

		
	}

	/**
	 * @param obj
	 * @param data
	 * @param bins 
	 * @param map
	 */
	protected void writeMapInternal(Map<Object, Object> obj, AerospikeData data,	TypeInformation<?> propertyType, List<Bin> bins) {
			
		
	}

	/**
	 * A {@link PropertyValueProvider} to lookup values on the configured {@link Record}.
	 *
	 * @author Oliver Gierke
	 */
	private static class RecordReadingPropertyValueProvider implements PropertyValueProvider<AerospikePersistentProperty> {

		private final Record record;
		private final ConversionService conversionService;
		private final SimpleTypeHolder simpleTypeHolder;

		/**
		 * Creates a new {@link RecordReadingPropertyValueProvider} for the given {@link Record}.
		 * 
		 * @param record must not be {@literal null}.
		 * @param conversionService 
		 */
		public RecordReadingPropertyValueProvider(Record record, ConversionService conversionService,SimpleTypeHolder simpleTypeHolder) {
			this.record = record;
			this.conversionService = conversionService;
			this.simpleTypeHolder = simpleTypeHolder;
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mapping.model.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public <T> T getPropertyValue(AerospikePersistentProperty property) {
			T value = null;
			if (record == null) return value;
			Object propertyObject =  record.getValue(((CachingAerospikePersistentProperty)property).getFieldName());
			
			if( propertyObject!=null){
				value = (T) conversionService.convert(propertyObject,TypeDescriptor.valueOf(propertyObject.getClass()) ,TypeDescriptor.valueOf(property.getType()));				
			}
			return value;
		}
		
	
		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mapping.model.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
		 */
		@SuppressWarnings("unchecked")
		public <T> T getPropertyValue(AerospikePersistentProperty property,Object propertyObject) {
			T value = null;
			if (record == null) return value;
			if(propertyObject!=null){
				value = (T) conversionService.convert(propertyObject,TypeDescriptor.valueOf(propertyObject.getClass()) ,TypeDescriptor.valueOf(property.getType()));				
			}
			return value;
		}
	}
	
	private static class AerospikeDataToProperty {
		
		

		/**
		 * @param <T>
		 * @param record
		 * @param property
		 * @return
		 */
		@SuppressWarnings("unchecked")
		public static <T> T convertRecordValueToProperty(Record record,AerospikePersistentProperty property) {
			final ConversionService conversionService = new DefaultConversionService();
			Assert.notNull(record, "record must not be null");
			Assert.notNull(property,"AerospikePersistentProperty must not be null");
			T value = (T) record.getValue(property.getName());
			if (value != null) {
				value = (T) conversionService.convert(value,TypeDescriptor.valueOf(value.getClass()) ,TypeDescriptor.valueOf(property.getActualType()));
//				Class<T> targetClass = (Class<T>) property.getActualType();
//				if (value.getClass().isAssignableFrom(targetClass) == false) {
//					if (targetClass.equals(Integer.class)) {
//						value = (T) (Integer) record.getInt(property.getName());
//					} else if (targetClass.equals(Double.class)) {
//						value = (T) (Double) record.getDouble(property.getName());
//					} else if (targetClass.equals(Byte.class)) {
//						value = (T) (Byte) record.getByte(property.getName());
//					} else if (targetClass.equals(Float.class)) {
//						value = (T) (Float) record.getFloat(property.getName());
//					} else if (targetClass.equals(Short.class)) {
//						value = (T) (Short) record.getShort(property.getName());
//					} else if (targetClass.equals(Long.class)) {
//						value = (T) (Long) record.getLong(property.getName());
//					} else
//						value = (T) record.getValue(property.getName());
//				}
			}

			return value;
		}
		
	}

	private static enum AerospikeTypeAliasAccessor implements TypeAliasAccessor<AerospikeData> {

		INSTANCE;

		private static final String TYPE_BIN_NAME = "spring_class";

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.convert.TypeAliasAccessor#readAliasFrom(java.lang.Object)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public Object readAliasFrom(AerospikeData source) {
			Assert.notNull(source);
			if (source.getRecord() == null) return null;
			return source.getMetaData()==null?null:source.getMetaData().getAerospikeMetaDataUsingKey(TYPE_BIN_NAME);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.convert.TypeAliasAccessor#writeTypeTo(java.lang.Object, java.lang.Object)
		 */
		@Override
		public void writeTypeTo(AerospikeData sink, Object alias) {
			sink.addMetaDataItem(TYPE_BIN_NAME, alias.toString());
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.core.AerospikeWriter#convertToAerospikeType(java.lang.Object)
	 */
	@Override
	public Object convertToAerospikeType(Object obj) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.core.AerospikeWriter#convertToAerospikeType(java.lang.Object, org.springframework.data.util.TypeInformation)
	 */
	@Override
	public Object convertToAerospikeType(Object obj,
			TypeInformation<?> typeInformation) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.core.AerospikeWriter#toAerospikeData(java.lang.Object, org.springframework.data.aerospike.mapping.AerospikePersistentProperty)
	 */
	@Override
	public AerospikeData toAerospikeData(Object object,
			AerospikePersistentProperty referingProperty) {
		// TODO Auto-generated method stub
		return null;
	}
	



}
