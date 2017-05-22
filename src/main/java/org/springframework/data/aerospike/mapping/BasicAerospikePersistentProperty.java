/*******************************************************************************
 * Copyright (c) 2015 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.springframework.data.aerospike.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.annotation.Expiration;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.*;
import org.springframework.util.StringUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BasicAerospikePersistentProperty extends AnnotationBasedPersistentProperty<AerospikePersistentProperty> implements
		AerospikePersistentProperty {

	private static final Logger LOG = LoggerFactory.getLogger(BasicAerospikePersistentProperty.class);

	private static final String ID_FIELD_NAME = "_id";
	private static final Set<Class<?>> SUPPORTED_ID_TYPES = new HashSet<Class<?>>();
	private static final Set<String> SUPPORTED_ID_PROPERTY_NAMES = new HashSet<String>();

	static {

		SUPPORTED_ID_TYPES.add(String.class);
		SUPPORTED_ID_TYPES.add(Integer.class);
		SUPPORTED_ID_TYPES.add(Long.class);
		SUPPORTED_ID_TYPES.add(byte[].class);
		SUPPORTED_ID_TYPES.add(Map.class);
		SUPPORTED_ID_TYPES.add(List.class);

		SUPPORTED_ID_PROPERTY_NAMES.add("id");
		SUPPORTED_ID_PROPERTY_NAMES.add(ID_FIELD_NAME);
	}

	private final FieldNamingStrategy fieldNamingStrategy;

	public BasicAerospikePersistentProperty(Field field,
											PropertyDescriptor propertyDescriptor,
											PersistentEntity<?, AerospikePersistentProperty> owner,
											SimpleTypeHolder simpleTypeHolder, FieldNamingStrategy fieldNamingStrategy) {
		super(field, propertyDescriptor, owner, simpleTypeHolder);

		this.fieldNamingStrategy = fieldNamingStrategy == null ? PropertyNameFieldNamingStrategy.INSTANCE
				: fieldNamingStrategy;

		if (isIdProperty() && getFieldName() != ID_FIELD_NAME) {
			LOG.warn("Customizing field name for id property not allowed! Custom name will not be considered!");
		}
	}

	/**
	 * Also considers fields as id that are of supported id type and name.
	 *
	 */
	@Override
	public boolean isIdProperty() {

		if (super.isIdProperty()) {
			return true;
		}

		// We need to support a wider range of ID types than just the ones that can be converted to an ObjectId
		// but still we need to check if there happens to be an explicit name set
		return SUPPORTED_ID_PROPERTY_NAMES.contains(getName()) && !hasExplicitFieldName();
	}

	@Override
	public boolean isExplicitIdProperty() {
		return isAnnotationPresent(Id.class);
	}

	@Override
	public boolean isExpirationProperty() {
		return isAnnotationPresent(Expiration.class);
	}

	/**
	 * Returns the key to be used to store the value of the property {@link DBObject}.
	 *
	 * @return
	 */
	public String getFieldName() {

		if (isIdProperty()) {

			if (owner == null) {
				return ID_FIELD_NAME;
			}

			if (owner.getIdProperty() == null) {
				return ID_FIELD_NAME;
			}

			if (owner.isIdProperty(this)) {
				return ID_FIELD_NAME;
			}
		}

		if (hasExplicitFieldName()) {
			return getAnnotatedFieldName();
		}

		String fieldName = fieldNamingStrategy.getFieldName(this);

		if (!StringUtils.hasText(fieldName)) {
			throw new MappingException(String.format("Invalid (null or empty) field name returned for property %s by %s!",
					this, fieldNamingStrategy.getClass()));
		}

		return fieldName;
	}

	protected boolean hasExplicitFieldName() {
		return StringUtils.hasText(getAnnotatedFieldName());
	}

	private String getAnnotatedFieldName() {

		org.springframework.data.aerospike.mapping.Field annotation = findAnnotation(org.springframework.data.aerospike.mapping.Field.class);
		if (annotation != null && StringUtils.hasText(annotation.value())) {
			return annotation.value();
		}
		return null;
	}

	@Override
	protected Association<AerospikePersistentProperty> createAssociation() {
		return new Association<AerospikePersistentProperty>(this, null);
	}

}
