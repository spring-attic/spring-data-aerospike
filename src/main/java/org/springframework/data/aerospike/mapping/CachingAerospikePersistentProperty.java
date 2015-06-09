package org.springframework.data.aerospike.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;

/**
 * 
 * @author Peter Milne
 *
 */
public class CachingAerospikePersistentProperty extends BasicAerospikePersistentProperty{

	private Boolean isIdProperty;
	private Boolean isAssociation;
	private String fieldName;
	private Boolean usePropertyAccess;
	private Boolean isTransient;

	/**
	 * Creates a new {@link CachingAerospikePersistentProperty}.
	 * 
	 * @param field
	 * @param propertyDescriptor
	 * @param owner
	 * @param simpleTypeHolder
	 * @param fieldNamingStrategy
	 */
	public CachingAerospikePersistentProperty(Field field,
			PropertyDescriptor propertyDescriptor,
			PersistentEntity<?, AerospikePersistentProperty> owner,
			SimpleTypeHolder simpleTypeHolder,
			FieldNamingStrategy fieldNamingStrategy) {
		super(field, propertyDescriptor, owner, simpleTypeHolder, fieldNamingStrategy);
	}
	@Override
	public boolean isIdProperty() {

		if (this.isIdProperty == null) {
			this.isIdProperty = super.isIdProperty();
		}

		return this.isIdProperty;
	}
	@Override
	public boolean isAssociation() {
		if (this.isAssociation == null) {
			this.isAssociation = super.isAssociation();
		}
		return this.isAssociation;
	}
	@Override
	public String getFieldName() {

		if (this.fieldName == null) {
			this.fieldName = super.getFieldName();
		}

		return this.fieldName;
	}
	@Override
	public boolean usePropertyAccess() {

		if (this.usePropertyAccess == null) {
			this.usePropertyAccess = super.usePropertyAccess();
		}

		return this.usePropertyAccess;
	}
	@Override
	public boolean isTransient() {

		if (this.isTransient == null) {
			this.isTransient = super.isTransient();
		}

		return this.isTransient;
	}

}
