package org.springframework.data.aerospike.convert;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.aerospike.SampleClasses.*;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikeSimpleTypes;

import java.util.*;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.AsCollections.*;
import static org.springframework.data.aerospike.SampleClasses.SimpleClass.SIMPLESET;
import static org.springframework.data.aerospike.SampleClasses.SimpleClassWithPersistenceConstructor.SIMPLESET2;
import static org.springframework.data.aerospike.SampleClasses.User.SIMPLESET3;

public class MappingAerospikeConverterTest {

	private static final String NAMESPACE = "namespace";
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private AerospikeMappingContext mappingContext = new AerospikeMappingContext();
	private List<Converter<?, ?>> converters = asList(new ComplexIdToStringConverter(), new StringToComplexIdConverter());
	private CustomConversions customConversions = new CustomConversions(converters, AerospikeSimpleTypes.HOLDER);
	private MappingAerospikeConverter converter = new MappingAerospikeConverter(mappingContext, customConversions);

	@Before
	public void setUp() throws Exception {
		mappingContext.setDefaultNameSpace(NAMESPACE);
		converter.afterPropertiesSet();
	}

	@Test
	public void shouldReadNullObjectIfAerospikeDataNull() throws Exception {
		SimpleClass actual = converter.read(SimpleClass.class, null);

		assertThat(actual).isEqualTo(null);
	}

	@Test
	public void shouldWriteSetWithSimpleValue() throws Exception {
		SetWithSimpleValue object = new SetWithSimpleValue(1L, set("a", "b", "c", null));
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThat(forWrite.getBins()).containsOnly(
				new Bin("collectionWithSimpleValues", list(null, "a", "b", "c")),
				new Bin("@user_key", "1"),
				new Bin("@_class", SetWithSimpleValue.class.getName())
		);

	}

	@Test
	public void shouldReadCollectionWithSimpleValue() throws Exception {
		Map<String, Object> bins = of(
				"collectionWithSimpleValues", list("a", "b", "c", "d", null),
				"@user_key", "10"
		);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), bins);

		SetWithSimpleValue actual = converter.read(SetWithSimpleValue.class, forRead);

		Set<String> map = set("a", "b", "c", "d", null);
		SetWithSimpleValue expected = new SetWithSimpleValue(10L, map);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteMapWithSimpleValue() throws Exception {
		Map<String, String> map = of("key1", "value1", "key2", "value2");
		MapWithSimpleValue object = new MapWithSimpleValue(10L, map);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThatKeyIsEqualTo(forWrite.getKey(), NAMESPACE, MapWithSimpleValue.class.getSimpleName(), 10L);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("mapWithSimpleValue", of("key1", "value1", "key2", "value2")),
				new Bin("@user_key", "10"),
				new Bin("@_class", MapWithSimpleValue.class.getName())
		);
	}

	@Test
	public void shouldReadMapWithSimpleValue() throws Exception {
		Map<String, Object> bins = of(
				"mapWithSimpleValue", of("key1", "value1", "key2", "value2"),
				"@user_key", "10"
		);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), bins);

		MapWithSimpleValue actual = converter.read(MapWithSimpleValue.class, forRead);

		Map<String, String> map = of("key1", "value1", "key2", "value2");
		MapWithSimpleValue expected = new MapWithSimpleValue(10L, map);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteMapWithCollectionValues() throws Exception {
		Map<String, List<String>> map = of("key1", list(), "key2", list("a", "b", "c"));
		MapWithCollectionValue object = new MapWithCollectionValue(10L, map);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThatKeyIsEqualTo(forWrite.getKey(), NAMESPACE, MapWithCollectionValue.class.getSimpleName(), 10L);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("mapWithCollectionValue", of("key1", list(), "key2", list("a", "b", "c"))),
				new Bin("@user_key", "10"),
				new Bin("@_class", MapWithCollectionValue.class.getName())
		);
	}

	@Test
	public void shouldReadMapWithCollectionValues() throws Exception {
		Map<String, Object> bins = of(
				"mapWithCollectionValue", of("key1", list(), "key2", list("a", "b", "c")),
				"@user_key", "10"
		);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), bins);

		MapWithCollectionValue actual = converter.read(MapWithCollectionValue.class, forRead);

		Map<String, List<String>> map = of("key1", list(), "key2", list("a", "b", "c"));
		MapWithCollectionValue expected = new MapWithCollectionValue(10L, map);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteMapWithNonSimpleValue() throws Exception {
		Map<String, Address> map = of("key1", new Address(new Street("Gogolya str.", 15), 567),
				"key2", new Address(new Street("Shakespeare str.", 40), 765));
		MapWithNonSimpleValue object = new MapWithNonSimpleValue(10L, map);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThatKeyIsEqualTo(forWrite.getKey(), NAMESPACE, MapWithNonSimpleValue.class.getSimpleName(), 10L);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("mapWithNonSimpleValue", of(
						"key1", of("street", of("name", "Gogolya str.", "number", 15, "@_class", Street.class.getName()),
								"apartment", 567, "@_class", Address.class.getName()),
						"key2", of("street", of("name", "Shakespeare str.", "number", 40, "@_class", Street.class.getName()),
								"apartment", 765, "@_class", Address.class.getName()))),
				new Bin("@user_key", "10"),
				new Bin("@_class", MapWithNonSimpleValue.class.getName())
		);
	}

	@Test
	public void shouldReadMapWithNonSimpleValue() throws Exception {
		Map<String, Object> bins = of(
				"mapWithNonSimpleValue", of(
						"key1", of("street", of("name", "Gogolya str.", "number", 15), "apartment", 567),
						"key2", of("street", of("name", "Shakespeare str.", "number", 40), "apartment", 765)),
				"@user_key", "10"
		);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), bins);

		MapWithNonSimpleValue actual = converter.read(MapWithNonSimpleValue.class, forRead);

		Map<String, Address> map = of("key1", new Address(new Street("Gogolya str.", 15), 567),
				"key2", new Address(new Street("Shakespeare str.", 40), 765));
		MapWithNonSimpleValue expected = new MapWithNonSimpleValue(10L, map);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteObjectWithSimpleFields() throws Exception {
		Set<String> field9 = set("val1", "val2");
		Set<Set<String>> field10 = set(set("1", "2"), set("3", "4"), set());
		SimpleClass object = new SimpleClass(0, "abyrvalg", 13, 14L, (float) 15, 16.0, true, new Date(8878888),
				TYPES.SECOND, field9, field10);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThatKeyIsEqualTo(forWrite.getKey(), NAMESPACE, SIMPLESET, 0);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("field1", "abyrvalg"),
				new Bin("field2", 13),
				new Bin("field3", 14L),
				new Bin("field4", (float) 15),
				new Bin("field5", 16.0),
				new Bin("field6", true),
				new Bin("field7", 8878888L),
				new Bin("field8", "SECOND"),
				new Bin("field9", list("val2", "val1")),
				new Bin("field10", list(list(), list("1", "2"), list("3", "4"))),
				new Bin("@_class", "simpleclass"),
				new Bin("@user_key", "0")
		);
	}

	@Test
	public void shouldReadObjectWithSimpleFields() throws Exception {
		Map<String, Object> bins = new HashMap<>();
		bins.put("field1", "abyrvalg");
		bins.put("field2", 13);
		bins.put("field3", 14L);
		bins.put("field4", (float) 15);
		bins.put("field5", 16.0);
		bins.put("field6", true);
		bins.put("field7", 77777L);
		bins.put("field8", "SECOND");
		bins.put("field9", list("val1", "val2"));
		bins.put("field10", list(list(), list("1", "2"), list("3", "4")));
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 867), bins);

		SimpleClass actual = converter.read(SimpleClass.class, forRead);

		assertThat(actual).isEqualTo(new SimpleClass(867, "abyrvalg", 13, 14L, (float) 15, 16.0, true,
				new Date(77777L), TYPES.SECOND, set("val1", "val2"), set(set("1", "2"), set("3", "4"), set())));
	}

	@Test
	public void shouldWriteObjectWithPersistenceConstructor() throws Exception {
		SimpleClassWithPersistenceConstructor object = new SimpleClassWithPersistenceConstructor(17, "abyrvalg", 13);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThatKeyIsEqualTo(forWrite.getKey(), NAMESPACE, SIMPLESET2, 17);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@user_key", "17"),
				new Bin("@_class", SimpleClassWithPersistenceConstructor.class.getName()),
				new Bin("field1", "abyrvalg"),
				new Bin("field2", 13));
	}

	@Test
	public void shouldReadObjectWithPersistenceConstructor() throws Exception {
		Map<String, Object> bins = new HashMap<>();
		bins.put("field1", "abyrvalg");
		bins.put("field2", 13);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 555), bins);

		SimpleClassWithPersistenceConstructor actual = converter.read(SimpleClassWithPersistenceConstructor.class, forRead);

		assertThat(actual).isEqualTo(new SimpleClassWithPersistenceConstructor(555, "abyrvalg", 13));
	}

	@Test
	public void shouldReadComplexClass() throws Exception {
		Map<String, Object> bins = of(
				"name",
				of("firstName", "Vasya", "lastName", "Pupkin"),
				"address",
				of("street",
						of("name", "Gogolya street", "number", 24),
						"apartment", 777)
		);
		AerospikeReadData data = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 555), bins);

		User actual = converter.read(User.class, data);

		Name name = new Name("Vasya", "Pupkin");
		Address address = new Address(new Street("Gogolya street", 24), 777);
		User expected = new User(555, name, address);
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteComplexClass() throws Exception {
		Name name = new Name("Vasya", "Pupkin");
		Address address = new Address(new Street("Gogolya street", 24), 777);
		User object = new User(10, name, address);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		assertThatKeyIsEqualTo(forWrite.getKey(), NAMESPACE, SIMPLESET3, 10);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@user_key", "10"),
				new Bin("@_class", User.class.getName()),
				new Bin("name",
						of("firstName", "Vasya", "lastName", "Pupkin", "@_class", Name.class.getName())),
				new Bin("address",
						of("street",
								of("name", "Gogolya street", "number", 24, "@_class", Street.class.getName()),
								"apartment", 777, "@_class", Address.class.getName()))
		);
	}

	@Test
	public void shouldWriteIntId() {
		ClassWithIntId value = new ClassWithIntId();
		value.id = 5;
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(value, forWrite);

		assertThatKeyIsEqualTo(forWrite.getKey(), NAMESPACE, "ClassWithIntId", 5);
		assertThat(forWrite.getBins()).contains(new Bin("@user_key", "5"));
	}

	@Test
	public void shouldReadIntId() throws Exception {
		Map<String, Object> data = of();

		ClassWithIntId actual = converter.read(ClassWithIntId.class, AerospikeReadData.forRead(new Key(NAMESPACE, "ClassWithIntId", 5), data));

		ClassWithIntId expected = new ClassWithIntId();
		expected.id = 5;
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void shouldWriteSetWithComplexValue() {
		Set<Address> addresses = set(
				new Address(new Street("Southwark Street", 110), 876),
				new Address(new Street("Finsbury Pavement", 125), 13));
		Person person = new Person("kate-01", addresses);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(person, forWrite);

		assertThatKeyIsEqualTo(forWrite.getKey(), NAMESPACE, "Person", "kate-01");
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", Person.class.getName()),
				new Bin("@user_key", "kate-01"),
				new Bin("addresses", list(
						of("street",
								of("name", "Southwark Street", "number", 110, "@_class", Street.class.getName()),
								"apartment", 876, "@_class", Address.class.getName()),
						of("street",
								of("name", "Finsbury Pavement", "number", 125, "@_class", Street.class.getName()),
								"apartment", 13, "@_class", Address.class.getName())
				)));
	}

	@Test
	public void shouldReadSetWithComplexValue() {
		Map<String, Object> bins = of(
				"addresses", list(
						of("street",
								of("name", "Southwark Street", "number", 110),
								"apartment", 876),
						of("street",
								of("name", "Finsbury Pavement", "number", 125),
								"apartment", 13)
				)
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "Person", "kate-01"), bins);

		Person result = converter.read(Person.class, dbObject);

		Set<Address> addresses = set(
				new Address(new Street("Southwark Street", 110), 876),
				new Address(new Street("Finsbury Pavement", 125), 13));
		Person person = new Person("kate-01", addresses);
		assertThat(result).isEqualTo(person);
	}

	@Test
	public void shouldWriteEnumProperties() throws Exception {
		List<TYPES> list = list(TYPES.FIRST, TYPES.SECOND);
		EnumSet<TYPES> set = EnumSet.allOf(TYPES.class);
		EnumMap<TYPES, String> map = new EnumMap<TYPES, String>(of(TYPES.FIRST, "a", TYPES.SECOND, "b"));
		EnumProperties object = new EnumProperties(TYPES.SECOND, list, set, map);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", EnumProperties.class.getName()),
				new Bin("type", "SECOND"),
				new Bin("list", list("FIRST", "SECOND")),
				new Bin("set", list("FIRST", "SECOND", "THIRD")),
				new Bin("map", of("FIRST", "a", "SECOND", "b"))
		);
	}

	@Test
	public void shouldReadEnumProperties() throws Exception {
		Map<String, Object> bins = of(
				"@_class", EnumProperties.class.getName(),
				"type", "SECOND",
				"list", list("FIRST", "SECOND"),
				"set", list("FIRST", "SECOND", "THIRD"),
				"map", of("FIRST", "a", "SECOND", "b")
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "EnumProperties", 10L), bins);

		EnumProperties result = converter.read(EnumProperties.class, dbObject);

		List<TYPES> list = list(TYPES.FIRST, TYPES.SECOND);
		EnumSet<TYPES> set = EnumSet.allOf(TYPES.class);
		EnumMap<TYPES, String> map = new EnumMap<TYPES, String>(of(TYPES.FIRST, "a", TYPES.SECOND, "b"));
		EnumProperties expected = new EnumProperties(TYPES.SECOND, list, set, map);
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteSortedMapWithSimpleValue() throws Exception {
		SortedMap<String, String> map = new TreeMap<>(of("a", "b", "c", "d"));
		SortedMapWithSimpleValue object = new SortedMapWithSimpleValue(map);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", SortedMapWithSimpleValue.class.getName()),
				new Bin("map", of("a", "b", "c", "d"))
		);
	}

	@Test
	public void shouldReadSortedMapWithSimpleValue() throws Exception {
		Map<String, Object> bins = of(
				"@_class", SortedMapWithSimpleValue.class.getName(),
				"map", of("a", "b", "c", "d")
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "SortedMapWithSimpleValue", 10L), bins);

		SortedMapWithSimpleValue result = converter.read(SortedMapWithSimpleValue.class, dbObject);

		SortedMap<String, String> map = new TreeMap<>(of("a", "b", "c", "d"));
		SortedMapWithSimpleValue expected = new SortedMapWithSimpleValue(map);
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteNestedMapsWithSimpleValue() throws Exception {
		Map<String, Map<String, Map<String, String>>> map = of(
				"level-1", of("level-1-1", of("1", "2")),
				"level-2", of("level-2-2", of("1", "2")));
		NestedMapsWithSimpleValue object = new NestedMapsWithSimpleValue(map);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", NestedMapsWithSimpleValue.class.getName()),
				new Bin("nestedMaps", of(
						"level-1", of("level-1-1", of("1", "2")),
						"level-2", of("level-2-2", of("1", "2"))))
		);
	}

	@Test
	public void shouldReadNestedMapsWithSimpleValue() throws Exception {
		Map<String, Object> bins = of(
				"@_class", NestedMapsWithSimpleValue.class.getName(),
				"nestedMaps", of(
						"level-1", of("level-1-1", of("1", "2")),
						"level-2", of("level-2-2", of("1", "2")))
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "NestedMapsWithSimpleValue", 10L), bins);

		NestedMapsWithSimpleValue result = converter.read(NestedMapsWithSimpleValue.class, dbObject);

		Map<String, Map<String, Map<String, String>>> map = of(
				"level-1", of("level-1-1", of("1", "2")),
				"level-2", of("level-2-2", of("1", "2")));
		NestedMapsWithSimpleValue expected = new NestedMapsWithSimpleValue(map);
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteGenericType() throws Exception {
		GenericType<GenericType<String>> object = new GenericType("string");

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", GenericType.class.getName()),
				new Bin("content", "string")
		);
	}

	@Test
	public void shouldReadGenericType() throws Exception {
		Map<String, Object> bins = of(
				  "@_class", GenericType.class.getName(),
				  "content", "string"
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "GenericType", 10L), bins);

		GenericType result = converter.read(GenericType.class, dbObject);

		GenericType expected = new GenericType("string");
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteListOfLists() throws Exception {
		ListOfLists object = new ListOfLists(list(list("a", "b", "c"), list("d", "e"), list()));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", ListOfLists.class.getName()),
				new Bin("listOfLists", list(list("a", "b", "c"), list("d", "e"), list()))
		);
	}

	@Test
	public void shouldReadListOfLists() throws Exception {
		Map<String, Object> bins = of(
				"@_class", ListOfLists.class.getName(),
				"listOfLists", list(list("a", "b", "c"), list("d", "e"), list())
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "ListOfLists", 10L), bins);

		ListOfLists result = converter.read(ListOfLists.class, dbObject);

		ListOfLists expected = new ListOfLists(list(list("a", "b", "c"), list("d", "e"), list()));
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteListOfMaps() throws Exception {
		ListOfMaps object = new ListOfMaps(list(of("vasya", new Name("Vasya", "Pukin")), of("nastya", new Name("Nastya", "Smirnova"))));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", ListOfMaps.class.getName()),
				new Bin("listOfMaps", list(
						of("vasya", of("firstName", "Vasya", "lastName", "Pukin", "@_class", Name.class.getName())),
						of("nastya", of("firstName", "Nastya", "lastName", "Smirnova", "@_class", Name.class.getName()))
				)));
	}

	@Test
	public void shouldReadListOfMaps() throws Exception {
		Map<String, Object> bins = of(
				"@_class", ListOfMaps.class.getName(),
				"listOfMaps", list(
						of("vasya", of("firstName", "Vasya", "lastName", "Pukin", "@_class", Name.class.getName())),
						of("nastya", of("firstName", "Nastya", "lastName", "Smirnova", "@_class", Name.class.getName()))
				)
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "ListOfMaps", 10L), bins);

		ListOfMaps result = converter.read(ListOfMaps.class, dbObject);

		ListOfMaps expected = new ListOfMaps(list(of("vasya", new Name("Vasya", "Pukin")), of("nastya", new Name("Nastya", "Smirnova"))));
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteContainerOfCustomFieldNames() throws Exception {
		ContainerOfCustomFieldNames object = new ContainerOfCustomFieldNames("value", new CustomFieldNames(1, "2"));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThat(forWrite.getKey()).isNull();
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", ContainerOfCustomFieldNames.class.getName()),
				new Bin("property", "value"),
				new Bin("customFieldNames", of("property1", 1, "property2", "2", "@_class", CustomFieldNames.class.getName()))
		);
	}

	@Test
	public void shouldReadContainerOfCustomFieldNames() throws Exception {
		Map<String, Object> bins = of(
				"@_class", ContainerOfCustomFieldNames.class.getName(),
				"property", "value",
				"customFieldNames", of("property1", 1, "property2", "2", "@_class", CustomFieldNames.class.getName())
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "ContainerOfCustomFieldNames", 10L), bins);

		ContainerOfCustomFieldNames result = converter.read(ContainerOfCustomFieldNames.class, dbObject);

		ContainerOfCustomFieldNames expected = new ContainerOfCustomFieldNames("value", new CustomFieldNames(1, "2"));
		assertThat(result).isEqualTo(expected);
	}

	@Test
	public void shouldWriteClassWithComplexId() throws Exception {
		ClassWithComplexId object = new ClassWithComplexId(new ComplexId(10L));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(object, forWrite);

		assertThatKeyIsEqualTo(forWrite.getKey(), NAMESPACE, ClassWithComplexId.class.getSimpleName(), "id::10");
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@_class", ClassWithComplexId.class.getName()),
				new Bin("@user_key", "id::10")
		);
	}

	@Test
	public void shouldReadClassWithComplexId() throws Exception {
		Map<String, Object> bins = of(
				"@_class", ClassWithComplexId.class.getName(),
				"@user_key", "id::10"
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "ClassWithComplexId", 10L), bins);

		ClassWithComplexId result = converter.read(ClassWithComplexId.class, dbObject);

		ClassWithComplexId expected = new ClassWithComplexId(new ComplexId(10L));
		assertThat(result).isEqualTo(expected);
	}

	private void assertThatKeyIsEqualTo(Key key, String namespace, String myset, Object expected) {
		assertThat(key.namespace).isEqualTo(namespace);
		assertThat(key.setName).isEqualTo(myset);
		assertThat(key.userKey.getObject()).isEqualTo(String.valueOf(expected));
	}


}