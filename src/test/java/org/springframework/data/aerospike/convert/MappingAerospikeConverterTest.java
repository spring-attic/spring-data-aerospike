/**
 *
 */
package org.springframework.data.aerospike.convert;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.util.*;

import org.hamcrest.MatcherAssert;
import org.hamcrest.beans.HasProperty;
import org.hamcrest.beans.SamePropertyValuesAs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.ConversionNotSupportedException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.aerospike.convert.MappingAerospikeConverterTest.ClassWithMapUsingEnumAsKey.FooBarEnum;
import org.springframework.data.aerospike.mapping.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class MappingAerospikeConverterTest {

	MappingAerospikeConverter converter;
	Key key;

	private static final String AEROSPIKE_KEY = "AerospikeKey";
	private static final String AEROSPIKE_SET_NAME = "AerospikeSetName";
	private static final String AEROSPIKE_NAME_SPACE = "AerospikeNameSpace";

	/**
	 * @throws java.lang.Exception
	 */

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);

		converter = new MappingAerospikeConverter(new AerospikeMappingContext(), AerospikeSimpleTypes.HOLDER);
		key = new Key(AEROSPIKE_NAME_SPACE, AEROSPIKE_SET_NAME, AEROSPIKE_KEY);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.convert.MappingAerospikeConverter#MappingAerospikeConverter()}.
	 */
	@Test
	public void testMappingAerospikeConverter() {
		MappingAerospikeConverter mappingAerospikeConverter = new MappingAerospikeConverter(new AerospikeMappingContext(), AerospikeSimpleTypes.HOLDER);
		assertNotNull(mappingAerospikeConverter.getMappingContext());
		assertNotNull(mappingAerospikeConverter.getConversionService());
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.convert.MappingAerospikeConverter#getMappingContext()}.
	 */
	@Test
	public void testGetMappingContext() {
		MappingAerospikeConverter mappingAerospikeConverter = new MappingAerospikeConverter(new AerospikeMappingContext(), AerospikeSimpleTypes.HOLDER);
		assertNotNull(mappingAerospikeConverter.getMappingContext());
		assertTrue(mappingAerospikeConverter.getMappingContext() instanceof AerospikeMappingContext);
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.convert.MappingAerospikeConverter#getConversionService()}.
	 */
	@Test
	public void testGetConversionService() {
		MappingAerospikeConverter mappingAerospikeConverter = new MappingAerospikeConverter(new AerospikeMappingContext(), AerospikeSimpleTypes.HOLDER);
		assertNotNull(mappingAerospikeConverter.getConversionService());
		assertTrue(mappingAerospikeConverter.getConversionService() instanceof DefaultConversionService);
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.convert.MappingAerospikeConverter#read(java.lang.Class, org.springframework.data.aerospike.convert.AerospikeData)}.
	 */
	@Test
	public void convertsAddressCorrectlyToAerospikeData() {
		Address address = new Address();
		address.city = "New York";
		address.street = "Broadway";

		AerospikeData dbObject = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		dbObject.setID(AEROSPIKE_KEY);
		converter.write(address, dbObject);

		assertTrue(dbObject.getBins().contains(new Bin("city", "New York")));
		assertTrue(dbObject.getBins().contains(new Bin("street", "Broadway")));
	}

	@SuppressWarnings("serial")
	@Test
	public void convertsAerospikeDataToAddressCorrectly() {
		Address address = new Address();
		address.city = "New York";
		address.street = "Broadway";

		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("city", "New York");
				put("street", "Broadway");
			}
		};
		Record record = new Record(bins, 1, 1);

		AerospikeData dbObject = AerospikeData.forRead(key, record);

		Address convertedAddress = converter.read(Address.class, dbObject);
		assertThat(convertedAddress, SamePropertyValuesAs.samePropertyValuesAs(address));
	}

	@Test
	public void convertsDateTypesCorrectly() {
		Person person = new Person();
		person.birthDate = new Date();

		AerospikeData dbObject = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		dbObject.setID(AEROSPIKE_KEY);
		converter.write(person, dbObject);

		returnBinPropertyValue(dbObject, "birthDate");

		assertThat(returnBinPropertyValue(dbObject, "birthDate"), is(instanceOf(Date.class)));
		Record record = new Record(listToMap(dbObject.getBins()), 1, 1);

		AerospikeData forRead = AerospikeData.forRead(key, record);

		Person result = converter.read(Person.class, forRead);
		assertThat(result.birthDate, is(notNullValue()));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.convert.MappingAerospikeConverter#write(java.lang.Object, org.springframework.data.aerospike.convert.AerospikeData)}.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void writesMapTypeCorrectly() {
		ClassWithMapProperty foo = new ClassWithMapProperty();

		foo.map = Collections.singletonMap(Locale.US, "Biff");

		AerospikeData dbObject = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		dbObject.setID(AEROSPIKE_KEY);
		converter.write(foo, dbObject);

		Object object = returnBinPropertyValue(dbObject, "map");
		assertThat((Map<Locale, String>) object, hasEntry(Locale.US, "Biff"));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.convert.MappingAerospikeConverter#write(java.lang.Object, org.springframework.data.aerospike.convert.AerospikeData)}.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void writesNullValuesForMapsCorrectly() {
		ClassWithMapProperty foo = new ClassWithMapProperty();

		foo.map = Collections.singletonMap(Locale.US, null);

		AerospikeData dbObject = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		dbObject.setID(AEROSPIKE_KEY);
		converter.write(foo, dbObject);

		Object object = returnBinPropertyValue(dbObject, "map");
		assertThat((Map<Locale, String>) object, hasEntry(Locale.US, null));
	}

	@Test
	public void writesEnumsCorrectly() {
		ClassWithEnumProperty value = new ClassWithEnumProperty();
		value.sampleEnum = SampleEnum.FIRST;

		AerospikeData result = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		result.setID(AEROSPIKE_KEY);
		converter.write(value, result);

		Object object = returnBinPropertyValue(result, "sampleEnum");
		//all Enums are saved in form of String in the DB
		assertThat(object, is(instanceOf(String.class)));
		assertThat(SampleEnum.valueOf(object.toString()), is(SampleEnum.FIRST));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void writesEnumCollectionCorrectly() {
		ClassWithEnumProperty value = new ClassWithEnumProperty();
		value.enums = Arrays.asList(SampleEnum.FIRST);

		AerospikeData result = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		result.setID(AEROSPIKE_KEY);
		converter.write(value, result);

		Object object = returnBinPropertyValue(result, "enums");

		assertThat(((List) object).size(), is(1));
		assertThat((String) ((List) object).get(0).toString(), is("FIRST"));
	}

	@SuppressWarnings("serial")
	@Test
	public void readsEnumsCorrectly() {
		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("sampleEnum", SampleEnum.FIRST);
			}
		};
		Record record = new Record(bins, 1, 1);

		AerospikeData dbObject = AerospikeData.forRead(key, record);

		ClassWithEnumProperty result = converter.read(ClassWithEnumProperty.class, dbObject);

		assertThat(result.sampleEnum, is(SampleEnum.FIRST));
	}

	@SuppressWarnings("serial")
	@Test
	public void readsEnumCollectionsCorrectly() {
		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("sampleEnum", SampleEnum.FIRST);
				put("enums", Arrays.asList(SampleEnum.FIRST));
			}
		};
		Record record = new Record(bins, 1, 1);

		AerospikeData dbObject = AerospikeData.forRead(key, record);

		ClassWithEnumProperty result = converter.read(ClassWithEnumProperty.class, dbObject);

		assertThat(result.enums, is(instanceOf(List.class)));
		assertThat(result.enums.size(), is(1));
		assertThat(result.enums, hasItem(SampleEnum.FIRST));
	}

	@Test
	public void considersFieldNameAnnotationWhenWriting() {
		Person person = new Person();
		person.firstname = "Oliver";

		AerospikeData dbObject = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		dbObject.setID(AEROSPIKE_KEY);
		converter.write(person, dbObject);


		Object foo = returnBinPropertyValue(dbObject, "foo");
		Object firstName = returnBinPropertyValue(dbObject, "firstName");

		MatcherAssert.assertThat((String) foo, is(equalTo("Oliver")));
		assertNull(firstName);
	}

	@SuppressWarnings("serial")
	@Test
	public void considersFieldNameAnnotationWhenReading() {
		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("id", "id1");
				put("birthDate", null);
				put("foo", "Oliver");
			}
		};
		Record record = new Record(bins, 1, 1);

		AerospikeData dbObject = AerospikeData.forRead(key, record);

		Person result = converter.read(Person.class, dbObject);

		assertThat(result.firstname, is("Oliver"));
		assertThat(result, not(HasProperty.hasProperty("foo")));
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void resolvesNestedComplexTypeForWriteCorrectly() {
		Address address = new Address();
		address.city = "London";
		address.street = "110 Southwark Street";

		Set<Address> addresses = new HashSet<MappingAerospikeConverterTest.Address>();
		addresses.add(address);
		Person person = new Person(addresses);

		AerospikeData dbObject = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		dbObject.setID(AEROSPIKE_KEY);
		converter.write(person, dbObject);

		List<Object> list = (List<Object>) returnBinPropertyValue(dbObject, "addresses");

		HashMap hashMap = (HashMap) list.get(0);
		String city = (String) hashMap.get("city");
		assertThat(city, is("London"));
	}

	@SuppressWarnings("serial")
	@Test
	public void resolvesNestedComplexTypeForReadCorrectly() {
		final Address address = new Address();
		address.city = "London";
		address.street = "110 Southwark Street";

		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("id", "id1");
				put("birthDate", new Date());
				put("foo", "Oliver");
				put("lastname", "Cromwell");
				put("addresses", address);
			}
		};
		Record record = new Record(bins, 1, 1);

		AerospikeData dbObject = AerospikeData.forRead(key, record);

		Person result = converter.read(Person.class, dbObject);

		assertThat(result.addresses, is(notNullValue()));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void writesClassWithBigDecimal() {

		BigDecimalContainer container = new BigDecimalContainer();
		container.value = BigDecimal.valueOf(2.5d);
		container.map = Collections.singletonMap("foo", container.value);

		AerospikeData dbObject = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		dbObject.setID(AEROSPIKE_KEY);
		converter.write(container, dbObject);

		returnBinPropertyValue(dbObject, "value");
		Object objectMap = returnBinPropertyValue(dbObject, "map");

		assertThat(((Map<String, BigDecimal>) objectMap).get("foo"), is(instanceOf(BigDecimal.class)));
	}

	@SuppressWarnings("serial")
	@Test
	public void readClassWithBigDecimal() {
		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("value", BigDecimal.valueOf(2.5d));
				put("map", Collections.singletonMap("foo", BigDecimal.valueOf(2.5d)));
				put("collection", Arrays.asList(BigDecimal.valueOf(2.5d), BigDecimal.valueOf(12.5d), BigDecimal.valueOf(22.5d)));
			}
		};
		Record record = new Record(bins, 1, 1);

		AerospikeData dbObject = AerospikeData.forRead(key, record);

		BigDecimalContainer result = converter.read(BigDecimalContainer.class, dbObject);

		assertThat(result.value, is(BigDecimal.valueOf(2.5d)));
		assertThat(result.map.get("foo"), is(BigDecimal.valueOf(2.5d)));
		assertThat(result.collection.get(0), is(BigDecimal.valueOf(2.5d)));
	}

	@Test
	public void readsEmptySetsCorrectly() {
		Person person = new Person();
		person.addresses = Collections.emptySet();

		AerospikeData forWrite = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		forWrite.setID(AEROSPIKE_KEY);

		converter.write(person, forWrite);
		Record record = new Record(listToMap(forWrite.getBins()), 1, 1);

		AerospikeData forRead = AerospikeData.forRead(key, record);
		Person result = converter.read(Person.class, forRead);

		assertThat(result.addresses, hasSize(0));
	}

	@Test
	public void convertsObjectIdStringsToObjectIdCorrectly() {
		PersonPojoStringId p1 = new PersonPojoStringId("1234567890", "Text-1");
		AerospikeData dbObject = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		dbObject.setID(AEROSPIKE_KEY);

		converter.write(p1, dbObject);
		assertThat(returnBinPropertyValue(dbObject, MappingAerospikeConverter.SPRING_ID_BIN), is(instanceOf(String.class)));
	}

	@SuppressWarnings("serial")
	@Test
	public void convertsCustomEmptyMapCorrectly() {
		final Map<String, Object> map = new HashMap<String, Object>() {
			{
				put("city", "New York");
				put("street", "Broadway");
			}
		};

		Map<String, Object> bins = new HashMap<String, Object>() {
			{
				put("map", map);
			}
		};
		Record record = new Record(bins, 1, 1);
		AerospikeData dbObject = AerospikeData.forRead(key, record);

		ClassWithSortedMap result = converter.read(ClassWithSortedMap.class, dbObject);

		assertThat(result, is(instanceOf(ClassWithSortedMap.class)));
		assertThat(result.map, is(instanceOf(Map.class)));
	}

	@Test
	public void maybeConvertHandlesNullValuesCorrectly() {
		assertThat(converter.convertToAerospikeType(null), is(nullValue()));
	}

	@Test
	public void writesIntIdCorrectly() {
		ClassWithIntId value = new ClassWithIntId();
		value.id = 5;

		AerospikeData result = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		result.setID(AEROSPIKE_KEY);

		converter.write(value, result);

		returnBinPropertyValue(result, "_id");
		Object objectSpringValue = returnBinPropertyValue(result, MappingAerospikeConverter.SPRING_ID_BIN);

		assertThat(objectSpringValue, is((Object) 5));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void writesNullValuesForCollection() {
		CollectionWrapper wrapper = new CollectionWrapper();
		wrapper.contacts = Arrays.<Contact>asList(new Person(), null);

		AerospikeData result = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		result.setID(AEROSPIKE_KEY);

		converter.write(wrapper, result);

		List<Object> contacts = (List<Object>) returnBinPropertyValue(result, "contacts");
		assertThat(contacts, is(instanceOf(Collection.class)));
		assertThat(((Collection<?>) contacts).size(), is(2));
		Person contactItem = (Person) contacts.get(0);

		assertThat(contactItem.addresses, nullValue());
	}


	/**
	 * @param bins
	 * @return
	 */
	private Map<String, Object> listToMap(List<Bin> bins) {
		Map<String, Object> map = new HashMap<String, Object>();
		if (bins != null && bins.size() > 0) {
			for (Bin bin : bins) map.put(bin.name, bin.value.getObject());
		}
		return map;
	}

	/**
	 * @param aerospikeData
	 * @param property
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Object returnBinPropertyValue(AerospikeData aerospikeData, String property) {
		if (aerospikeData.getBins() == null || aerospikeData.getBins().size() == 0)
			return null;
		for (Iterator<Bin> iterator = aerospikeData.getBins().iterator(); iterator.hasNext(); ) {
			Bin bin = (Bin) iterator.next();

			if (bin.name.equals(AerospikeMetadataBin.AEROSPIKE_META_DATA)) {
				HashMap<String, Object> map = (HashMap<String, Object>) bin.value.getObject();
				for (Map.Entry<String, Object> entry : map.entrySet()) {
					if (entry.getKey().equals(property)) {
						return entry.getValue();
					}
				}
			} else if (bin.name.equals(property)) {
				return bin.value.getObject();
			}
		}
		return null;
	}

	static class GenericType<T> {
		T content;
	}

	static class ClassWithEnumProperty {

		SampleEnum sampleEnum;
		List<SampleEnum> enums;
		EnumSet<SampleEnum> enumSet;
		EnumMap<SampleEnum, String> enumMap;
	}

	static enum SampleEnum {
		FIRST {
			@Override
			void method() {
			}
		},
		SECOND {
			@Override
			void method() {

			}
		};

		abstract void method();
	}

	static interface InterfaceType {

	}

	static class Address implements InterfaceType {
		String street;
		String city;

		@Override
		public String toString() {
			return "Address [street=" + street + ", city=" + city + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((city == null) ? 0 : city.hashCode());
			result = prime * result
					+ ((street == null) ? 0 : street.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Address other = (Address) obj;
			if (city == null) {
				if (other.city != null)
					return false;
			} else if (!city.equals(other.city))
				return false;
			if (street == null) {
				if (other.street != null)
					return false;
			} else if (!street.equals(other.street))
				return false;
			return true;
		}
	}

	interface Contact {

	}

	static class Person implements Contact {
		@Id
		String id;

		Date birthDate;

		@Field("foo")
		String firstname;
		String lastname;

		Set<Address> addresses;

		public Person() {

		}

		public Person(Set<Address> addresses) {
			this.addresses = addresses;
		}
	}

	static class ClassWithSortedMap {
		SortedMap<String, String> map;
	}

	static class ClassWithMapProperty {
		Map<Locale, String> map;
		Map<String, List<String>> mapOfLists;
		Map<String, Object> mapOfObjects;
		Map<String, String[]> mapOfStrings;
		Map<String, Person> mapOfPersons;
		TreeMap<String, Person> treeMapOfPersons;
	}

	static class ClassWithNestedMaps {
		Map<String, Map<String, Map<String, String>>> nestedMaps;
	}

	static class BirthDateContainer {
		Date birthDate;
	}

	static class BigDecimalContainer {
		BigDecimal value;
		Map<String, BigDecimal> map;
		List<BigDecimal> collection;
	}

	static class CollectionWrapper {
		List<Contact> contacts;
		List<List<String>> strings;
		List<Map<String, Locale>> listOfMaps;
		Set<Contact> contactsSet;
	}

	static class LocaleWrapper {
		Locale locale;
	}

	static class ClassWithBigIntegerId {
		@Id
		BigInteger id;
	}

	static class A<T> {
		String valueType;
		T value;

		public A(T value) {
			this.valueType = value.getClass().getName();
			this.value = value;
		}
	}

	static class ClassWithIntId {
		@Id
		int id;
	}

	static class DefaultedConstructorArgument {
		String foo;
		int bar;
		double foobar;

		DefaultedConstructorArgument(String foo, int bar, double foobar) {
			this.foo = foo;
			this.bar = bar;
			this.foobar = foobar;
		}
	}

	static class Item {
		List<Attribute> attributes;
	}

	static class Attribute {
		String key;
		Object value;
	}

	static class Outer {
		class Inner {
			String value;
		}

		Inner inner;
	}

	static class URLWrapper {
		URL url;
	}

	static class ClassWithComplexId {
		@Id
		ComplexId complexId;
	}

	static class ComplexId {
		Long innerId;
	}

	static class TypWithCollectionConstructor {
		List<Attribute> attributes;

		public TypWithCollectionConstructor(List<Attribute> attributes) {
			this.attributes = attributes;
		}
	}

	@TypeAlias("_")
	static class Aliased {
		String name;
	}

	static class ThrowableWrapper {
		Throwable throwable;
	}

	@Document
	static class PrimitiveContainer {
		@Field("property")
		private final int m_property;

		public PrimitiveContainer(int a_property) {
			m_property = a_property;
		}

		public int property() {
			return m_property;
		}
	}

	@Document
	static class ObjectContainer {
		@Field("property")
		private final PrimitiveContainer m_property;

		public ObjectContainer(PrimitiveContainer a_property) {
			m_property = a_property;
		}

		public PrimitiveContainer property() {
			return m_property;
		}
	}

	static class RootForClassWithExplicitlyRenamedIdField {
		@Id
		String id;
		ClassWithExplicitlyRenamedField nested;
	}

	static class ClassWithExplicitlyRenamedField {
		@Field("id")
		String id;
	}

	static class RootForClassWithNamedIdField {
		String id;
		ClassWithNamedIdField nested;
	}

	static class ClassWithNamedIdField {
		String id;
	}

	static class ClassWithAnnotatedIdField {
		@Id
		String key;
	}

	static class ClassWithMapUsingEnumAsKey {
		static enum FooBarEnum {
			FOO, BAR;
		}

		Map<FooBarEnum, String> map;
	}

	static class FooBarEnumToStringConverter implements Converter<FooBarEnum, String> {
		@Override
		public String convert(FooBarEnum source) {

			if (source == null) {
				return null;
			}

			return FooBarEnum.FOO.equals(source) ? "foo-enum-value" : "bar-enum-value";
		}
	}

	static class StringToFooNumConverter implements Converter<String, FooBarEnum> {
		@Override
		public FooBarEnum convert(String source) {

			if (source == null) {
				return null;
			}

			if (source.equals("foo-enum-value")) {
				return FooBarEnum.FOO;
			}
			if (source.equals("bar-enum-value")) {
				return FooBarEnum.BAR;
			}

			throw new ConversionNotSupportedException(source, String.class, null);
		}
	}

}
