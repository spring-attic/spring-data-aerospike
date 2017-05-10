/**
 *
 */
package org.springframework.data.aerospike.convert;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.ConversionNotSupportedException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.aerospike.convert.MappingAerospikeConverterTest.ClassWithMapUsingEnumAsKey.FooBarEnum;
import org.springframework.data.aerospike.mapping.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mapping.model.SimpleTypeHolder;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class MappingAerospikeConverterConversionTest {
	MappingAerospikeConverter converter;
	Key key;

	private static final String AEROSPIKE_KEY = "AerospikeKey";
	private static final String AEROSPIKE_SET_NAME = "AerospikeSetName";
	private static final String AEROSPIKE_NAME_SPACE = "AerospikeNameSpace";

	private final SimpleTypeHolder simpleTypeHolder = AerospikeSimpleTypes.HOLDER;

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

	@SuppressWarnings("unchecked")
	@Test
	public void convertsAddressCorrectlyToAerospikeData() {
		Address address = new Address();
		address.city = "New York";
		address.street = "Broadway";

		AerospikeData dbObject = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		dbObject.setID(AEROSPIKE_KEY);
		converter.write(address, dbObject);

		HashMap<String, Object> map = (HashMap<String, Object>) AerospikeData.convertToMap(dbObject, simpleTypeHolder);
		AerospikeData dbObject2 = AerospikeData.convertToAerospikeData(map);
		converter.read(Person.class, dbObject2);

		assertTrue(dbObject.getBins().contains(new Bin("city", "New York")));
		assertTrue(dbObject.getBins().contains(new Bin("street", "Broadway")));
	}

	@SuppressWarnings("unchecked")
	public void convertsPersonAddressCorrectlyToAerospikeData() {
		Address address = new Address();
		address.city = "New York";
		address.street = "Broadway";

		AerospikeData dbObject = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		dbObject.setID(AEROSPIKE_KEY);
		converter.write(address, dbObject);

		HashMap<String, Object> map = (HashMap<String, Object>) AerospikeData.convertToMap(dbObject, simpleTypeHolder);
		AerospikeData dbObject2 = AerospikeData.convertToAerospikeData(map);
		converter.read(Person.class, dbObject2);

		assertTrue(dbObject.getBins().contains(new Bin("city", "New York")));
		assertTrue(dbObject.getBins().contains(new Bin("street", "Broadway")));
	}

	/**
	 * @param dbObject
	 * @param string
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

	@SuppressWarnings("unchecked")
	@Test
	public void resolvesNestedComplexTypeForWriteCorrectly() {
		Address address = new Address();
		address.city = "London";
		address.street = "110 Southwark Street";
		Address address2 = new Address();
		address2.city = "Toronto";
		address2.street = "110 West Side Street";

		Set<Address> addresses = new HashSet<Address>();
		addresses.add(address);
		addresses.add(address2);
		Person person = new Person(addresses);

		AerospikeData dbObject = AerospikeData.forWrite(AEROSPIKE_NAME_SPACE);
		dbObject.setID(AEROSPIKE_KEY);
		converter.write(person, dbObject);

		HashMap<String, Object> map = (HashMap<String, Object>) AerospikeData.convertToMap(dbObject, simpleTypeHolder);
		AerospikeData dbObject2 = AerospikeData.convertToAerospikeData(map);
		Person personReturned = converter.read(Person.class, dbObject2);
		assertThat(personReturned.getAddresses().size(), Matchers.is(2));

		returnBinPropertyValue(dbObject, "addresses");
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

		/**
		 * @return the addresses
		 */
		public Set<Address> getAddresses() {
			return addresses;
		}

		/**
		 * @param addresses the addresses to set
		 */
		public void setAddresses(Set<Address> addresses) {
			this.addresses = addresses;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((addresses == null) ? 0 : addresses.hashCode());
			result = prime * result
					+ ((birthDate == null) ? 0 : birthDate.hashCode());
			result = prime * result
					+ ((firstname == null) ? 0 : firstname.hashCode());
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result
					+ ((lastname == null) ? 0 : lastname.hashCode());
			return result;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Person other = (Person) obj;
			if (addresses == null) {
				if (other.addresses != null)
					return false;
			} else if (!addresses.equals(other.addresses))
				return false;
			if (birthDate == null) {
				if (other.birthDate != null)
					return false;
			} else if (!birthDate.equals(other.birthDate))
				return false;
			if (firstname == null) {
				if (other.firstname != null)
					return false;
			} else if (!firstname.equals(other.firstname))
				return false;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			if (lastname == null) {
				if (other.lastname != null)
					return false;
			} else if (!lastname.equals(other.lastname))
				return false;
			return true;
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
