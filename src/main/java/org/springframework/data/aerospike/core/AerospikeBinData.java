/**
 *
 */
package org.springframework.data.aerospike.core;

import java.io.Serializable;

/**
 * @param <T>
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeBinData<T> implements Serializable {

	private static final long serialVersionUID = 7235233996750282583L;

	private String propertyName;
	private Class<T> propertyType;
	private Object propertyValue;
	private boolean simpleType;

	/**
	 * @param propertyName
	 * @param propertyValue
	 * @param propertyType
	 * @param simpleType
	 */
	public AerospikeBinData(String propertyName, Object propertyValue, Class<T> propertyType, boolean simpleType) {
		this.propertyName = propertyName;
		this.propertyValue = propertyValue;
		this.propertyType = propertyType;
		this.simpleType = simpleType;

	}

	/**
	 * @return the propertyName
	 */
	public String getPropertyName() {
		return propertyName;
	}

	/**
	 * @param propertyName the propertyName to set
	 */
	public void setPropertyName(String propertyName) {
		this.propertyName = propertyName;
	}

	/**
	 * @return the propertyValue
	 */
	public Object getPropertyValue() {
		return propertyValue;
	}

	/**
	 * @param propertyValue the propertyValue to set
	 */
	public void setPropertyValue(Object propertyValue) {
		this.propertyValue = propertyValue;
	}

	/**
	 * @return the propertyType
	 */
	public Class<T> getPropertyType() {
		return propertyType;
	}

	/**
	 * @param propertyType the propertyType to set
	 */
	public void setPropertyType(Class<T> propertyType) {
		this.propertyType = propertyType;
	}

	/**
	 * @return the simpleType
	 */
	public boolean isSimpleType() {
		return simpleType;
	}

	/**
	 * @param simpleType the simpleType to set
	 */
	public void setSimpleType(boolean simpleType) {
		this.simpleType = simpleType;
	}

}
