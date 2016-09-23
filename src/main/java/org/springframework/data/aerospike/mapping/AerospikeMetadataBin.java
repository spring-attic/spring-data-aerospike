/**
 * 
 */
package org.springframework.data.aerospike.mapping;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.aerospike.client.Bin;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class AerospikeMetadataBin {

	public static final String AEROSPIKE_META_DATA = "ASpikeMetaData";
	public static final String TYPE_BIN_NAME = "spring_class";
	public static final String SPRING_ID_BIN = "SpringID";

	private Map<String, Object> map = new HashMap<String, Object>();

	public AerospikeMetadataBin() {
		// TODO Auto-generated constructor stub
	}

	@SuppressWarnings("unchecked")
	public void setAerospikeMetaDataBin(Bin bin){
		map = (Map<String, Object>) bin.value.getObject();
	}

	public Bin getAerospikeMetaDataBin(){
		return new Bin(AerospikeMetadataBin.AEROSPIKE_META_DATA, map);
	}

	public void addKeyValuetoAerospikeMetaData(String key, Object value){
		map.put(key, value);
	}

	public Object getAerospikeMetaDataUsingKey(String key){
		return map.get(key);
	}

	/**
	 * @param value
	 */
	public void addMap(HashMap<String, Object> map) {
		if(map==null)return;
		for(Entry<String, Object> entry : map.entrySet()) {
			addKeyValuetoAerospikeMetaData(entry.getKey(), entry.getValue());
		}		
	}

}
