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

import java.io.Serializable;
import java.util.*;

import org.springframework.data.aerospike.core.AerospikeBinData;
import org.springframework.data.aerospike.mapping.AerospikeMetadataBin;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.Assert;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;

/**
 * Value object to carry data to be read and to written in object conversion.
 * 
 * @author Oliver Gierke
 */
public class AerospikeData implements Serializable {

	private static final long serialVersionUID = 5008392909263724861L;

	private static final String AEROSPIKE_BIN_DATA_LIST = "AerospikeBinDataList";
	private static final String AEROSPIKE_BIN_RECORD_MAP = "AerospikeBinRecordMap";
	private static final String AEROSPIKE_KEY = "Aerospike_Key";
	public static final String SPRING_ID_BIN = "SpringID";
	private static final Collection<String> IGNORE = new HashSet<>(Arrays.asList(
			AerospikeMetadataBin.TYPE_BIN_NAME,
			AerospikeMetadataBin.SPRING_ID_BIN,
			AerospikeMetadataBin.AEROSPIKE_META_DATA,
			AerospikeData.AEROSPIKE_BIN_DATA_LIST,
			AerospikeData.AEROSPIKE_BIN_RECORD_MAP,
			AerospikeData.SPRING_ID_BIN));

	private Key key;
	private Record record;
	private final String namespace;
	private final List<Bin> bins;
	private AerospikeMetadataBin metaData;

	private AerospikeData(Key key, Record record, String namespace, List<Bin> bins, AerospikeMetadataBin metadata) {
		this.key = key;
		this.record = record;
		this.namespace = namespace;
		this.bins = bins;
		this.metaData = metadata;
	}

	public static AerospikeData forRead(Key key, Record record) {
		AerospikeMetadataBin metadata = new AerospikeMetadataBin();
		if (record != null) {
			metadata.addMap((HashMap<String, Object>) record.getValue(AerospikeMetadataBin.AEROSPIKE_META_DATA));
		}
		return new AerospikeData(key, record, key.namespace, Collections.<Bin>emptyList(), metadata);
	}

	public static AerospikeData forWrite(String namespace) {
		return new AerospikeData(null, null, namespace, new ArrayList<Bin>(), new AerospikeMetadataBin());
	}

	/**
	 * @return the key
	 */
	public Key getKey() {
		return key;
	}

	public void setID(byte[] ID){
		this.key = new Key(this.getNamespace(), this.getSetName(), ID);
	}

	public void setID(String ID){
		this.key = new Key(this.getNamespace(), this.getSetName(), ID);
	}

	public void setID(long ID){
		this.key = new Key(this.getNamespace(), this.getSetName(), ID);
	}

	public void setID(int ID){
		this.key = new Key(this.getNamespace(), this.getSetName(), ID);
	}

	public void setID(Value ID){
		this.key = new Key(this.getNamespace(), this.getSetName(), ID);
	}

	public void setID(Serializable id) {
		//getMetaData().addKeyValuetoAerospikeMetaData(AerospikeData.SPRING_ID_BIN, id);
		setID(id.toString());
	}

	/**
	 * @return the record
	 */
	public Record getRecord() {
		return record;
	}

	/**
	 * @return the namespace
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * @return the set name
	 */
	public String getSetName(){
		return (key != null) ? key.setName : null;
	}

	/**
	 * @return the bins
	 */
	public List<Bin> getBins() {
		return bins;
	}

	public Bin[] getBinsAsArray() {
		return getBins().toArray(new Bin[bins.size()]);
	}

	public void add(List<Bin> bins) {
		this.bins.addAll(bins);
	}

	public void add(Bin bin) {
		this.bins.add(bin);
	}

	public void addMetaDataToBin() {
		add(getMetaData().getAerospikeMetaDataBin());
	}

	public void addMetaDataItem(String key, Object value){
		Assert.notNull(key, "key must not be null");
		getMetaData().addKeyValuetoAerospikeMetaData(key, value);
	}

	public String[] getBinNames() {
		if (this.bins == null || this.bins.size()  == 0)
			return null;

		String[] binAsStringArray =  new String[bins.size()];
		int i = 0;
		for (Iterator<Bin> iterator = bins.iterator(); iterator.hasNext();) {
			Bin bin = (Bin) iterator.next();
			binAsStringArray[i] = bin.name;
			i++;
		}
		return binAsStringArray ;
	}

	public void setSetName(String setName) {
		this.key = new Key(this.getNamespace(), setName, this.key.userKey);
	}

	/**
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Object getSpringId() {
		HashMap<String, Object> aerospikeMetaData = (HashMap<String, Object>) record.getValue(AerospikeMetadataBin.AEROSPIKE_META_DATA);
		return aerospikeMetaData==null?null:aerospikeMetaData.get(SPRING_ID_BIN);
	}

	public AerospikeMetadataBin getMetaData() {
		return metaData;
	}

	public void setMetaData(AerospikeMetadataBin metaData) {
		this.metaData = metaData;
	}

	@SuppressWarnings({ "unused", "rawtypes" })
	public static Map convertToMap(AerospikeData aerospikeData, SimpleTypeHolder simpleTypeHolder){
		HashMap<String, Object> map = new HashMap<String, Object>(aerospikeData.bins.size() + 2);
		map.put(AerospikeMetadataBin.TYPE_BIN_NAME, aerospikeData.getMetaData().getAerospikeMetaDataUsingKey(AerospikeMetadataBin.TYPE_BIN_NAME));
		map.put(AerospikeMetadataBin.SPRING_ID_BIN, aerospikeData.getMetaData().getAerospikeMetaDataUsingKey(AerospikeMetadataBin.SPRING_ID_BIN));

		List<AerospikeBinData> binDatas =  new ArrayList<AerospikeBinData>();
		List<Bin> bins = aerospikeData.getBins();

		for (Bin bin : bins) {
			if(!bin.name.equals(AerospikeMetadataBin.AEROSPIKE_META_DATA)){
				map.put(bin.name, bin.value.getObject());
			}
		}
		return map;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static AerospikeData convertToAerospikeData(Map binMap) {
		Map<String, Object> map = (HashMap<String, Object>) binMap;
		if (map == null) {
			return null;
		}
		Key key = (Key) map.get(AerospikeData.AEROSPIKE_KEY);
		if (key == null) {
			key = new Key("namespace", "setname", "key");
		}

		Map<String, Object> recordBins = new HashMap<String, Object>();
		for (Map.Entry<String, Object> binEntry : map.entrySet()) {
			String property = binEntry.getKey();
			if (!IGNORE.contains(property)) {
				recordBins.put(property, binEntry.getValue());
			}
		}

		Record record = new Record(recordBins, 0, 0);
		AerospikeData aerospikeData = AerospikeData.forRead(key, record);
		return aerospikeData;
	}

}
