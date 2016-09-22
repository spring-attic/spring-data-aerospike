/**
 * 
 */
package org.springframework.data.aerospike.mapping;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import com.aerospike.client.Bin;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class AerospikeMetadataBinTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		 MockitoAnnotations.initMocks(this.getClass()); 
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.AerospikeMetadataBin#AerospikeMetadataBin()}.
	 */
	@Test
	public void testAerospikeMetadataBin() {
		AerospikeMetadataBin aerospikeMetadataBin = new AerospikeMetadataBin();
		assertNull(aerospikeMetadataBin.getAerospikeMetaDataUsingKey("biff"));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.AerospikeMetadataBin#setAerospikeMetaDataBin(com.aerospike.client.Bin)}.
	 */
	@Test
	public void testSetAerospikeMetaDataBin() {
		HashMap<String, Object> myMap;
		{
			myMap = new HashMap<String, Object>();
			myMap.put("one", "1");
			myMap.put("two", "2");
		};

		AerospikeMetadataBin aerospikeMetadataBin = new AerospikeMetadataBin();
		Bin bin = new Bin("biff", myMap);
		aerospikeMetadataBin.setAerospikeMetaDataBin(bin);
	
		assertThat(((Bin)aerospikeMetadataBin.getAerospikeMetaDataBin()).value.getObject(), instanceOf(Map.class));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.AerospikeMetadataBin#getAerospikeMetaDataBin()}.
	 */
	@Test
	public void testGetAerospikeMetaDataBin() {
		HashMap<String, Object> myMap;
		{
			myMap = new HashMap<String, Object>();
			myMap.put("one", "1");
			myMap.put("two", "2");
		};

		AerospikeMetadataBin aerospikeMetadataBin = new AerospikeMetadataBin();
		aerospikeMetadataBin.setAerospikeMetaDataBin(new Bin(AerospikeMetadataBin.AEROSPIKE_META_DATA,myMap));
		assertThat((Bin)aerospikeMetadataBin.getAerospikeMetaDataBin(),is(new Bin(AerospikeMetadataBin.AEROSPIKE_META_DATA,myMap)));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.AerospikeMetadataBin#addKeyValuetoAerospikeMetaData(java.lang.String, java.lang.Object)}.
	 */
	@Test
	public void testAddKeyValuetoAerospikeMetaData() {
		AerospikeMetadataBin aerospikeMetadataBin = new AerospikeMetadataBin();
		aerospikeMetadataBin.addKeyValuetoAerospikeMetaData("biff", "biffer");
		assertThat((String)aerospikeMetadataBin.getAerospikeMetaDataUsingKey("biff"), is("biffer"));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.AerospikeMetadataBin#getAerospikeMetaDataUsingKey(java.lang.String)}.
	 */
	@Test
	public void testGetAerospikeMetaDataUsingKey() {
		AerospikeMetadataBin aerospikeMetadataBin = new AerospikeMetadataBin();
		aerospikeMetadataBin.addKeyValuetoAerospikeMetaData("biff", "Aerospike");
		assertThat((String)aerospikeMetadataBin.getAerospikeMetaDataUsingKey("biff"), is("Aerospike"));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.AerospikeMetadataBin#addMap(java.util.HashMap)}.
	 */
	@Test
	public void testAddMap() {
		HashMap<String, Object> myMap;
		{
			myMap = new HashMap<String, Object>();
			myMap.put("one", "1");
			myMap.put("two", "2");
		};
		AerospikeMetadataBin aerospikeMetadataBin = new AerospikeMetadataBin();
		aerospikeMetadataBin.addMap(myMap);
		assertThat((String)aerospikeMetadataBin.getAerospikeMetaDataUsingKey("one"), is("1"));
	}

}
