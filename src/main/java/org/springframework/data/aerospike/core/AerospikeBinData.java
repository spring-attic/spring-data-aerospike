/**
 * 
 */
package org.springframework.data.aerospike.core;

import java.io.Serializable;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 * @param <T>
 *
 */
public class AerospikeBinData<T> implements Serializable{
	
	private String propertyName;
	private Class<T> propertyType;
	private Object propertyValue;
	
	/**
	 * @param name
	 * @param object
	 * @param aerospikeMetaDataUsingKey
	 */
	public AerospikeBinData(String propertyName, Object propertyValue,	Class<T> propertyType) {
		this.propertyName = propertyName;
		this.propertyValue = propertyValue;
		this.propertyType = propertyType;		
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
	
	

}
