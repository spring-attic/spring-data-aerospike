package org.springframework.data.aerospike.mapping;

import org.springframework.data.mapping.PersistentProperty;

public interface AerospikePersistentProperty extends PersistentProperty<AerospikePersistentProperty> {
	/**
	 * Returns whether property access shall be used for reading the property value. This means it will use the getter
	 * instead of field access.
	 * 
	 * @return
	 */
	boolean usePropertyAccess();
	boolean isExplicitIdProperty();

}
