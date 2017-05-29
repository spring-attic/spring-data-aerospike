/**
 *
 */
package org.springframework.data.aerospike.core;

import com.aerospike.client.*;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.helper.query.Qualifier;
import com.aerospike.helper.query.Qualifier.FilterOperation;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.SampleClasses.*;
import org.springframework.data.aerospike.repository.query.Criteria;
import org.springframework.data.aerospike.repository.query.Query;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class AerospikeTemplateTests extends BaseIntegrationTests {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	@Autowired AerospikeTemplate template;
	@Autowired AerospikeClient client;
	private String id;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.id = nextId();
		cleanDb();
	}

	private void cleanDb() {
		template.delete(Person.class);
		template.delete(VersionedClass.class);

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		cleanDb();
	}

	//test for RecordExistsAction.REPLACE_ONLY policy
	@Test
	public void shouldReplaceAllBinsPresentInAerospikeWhenSavingDocument() throws Exception {
		Key key = new Key(info.getNamespace(), "versioned-set", id);
		VersionedClass first = new VersionedClass(id, "foo");
		template.save(first);
		addNewFieldToSavedDataInAerospike(key);

		template.save(new VersionedClass(id, "foo2", 2));

		Record record2 = client.get(new Policy(), key);
		assertThat(record2.bins.get("notPresent")).isNull();
		assertThat(record2.bins.get("field")).isEqualTo("foo2");
	}

	private void addNewFieldToSavedDataInAerospike(Key key) {
		Record initial = client.get(new Policy(), key);
		Bin[] bins = Stream.concat(
				initial.bins.entrySet().stream().map(e -> new Bin(e.getKey(), e.getValue())),
				Stream.of(new Bin("notPresent", "cats"))).toArray(Bin[]::new);
		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.REPLACE;

		client.put(policy, key, bins);

		Record updated = client.get(new Policy(), key);
		assertThat(updated.bins.get("notPresent")).isEqualTo("cats");
	}

	@Test
	public void shouldSaveAndSetVersion() throws Exception {
		VersionedClass first = new VersionedClass(id, "foo");
		template.save(first);

		assertThat(first.version).isEqualTo(1);
		assertThat(template.findById(id, VersionedClass.class).version).isEqualTo(1);
	}

	@Test
	public void shouldNotSaveDocumentIfItAlreadyExistsWithZeroVersion() throws Exception {
		template.save(new VersionedClass(id, "foo", 0));

		expectedException.expect(OptimisticLockingFailureException.class);

		template.save(new VersionedClass(id, "foo", 0));
	}

	@Test
	public void shouldSaveDocumentWithEqualVersion() throws Exception {
		template.save(new VersionedClass(id, "foo", 0));

		template.save(new VersionedClass(id, "foo", 1));
		template.save(new VersionedClass(id, "foo", 2));
	}

	@Ignore("this does not work now because of: https://github.com/aerospike/aerospike-client-java/issues/74")
	@Test
	public void shouldFailSaveNewDocumentWithVersionGreaterThanZero() throws Exception {
		expectedException.expect(DataRetrievalFailureException.class);

		template.save(new VersionedClass(id, "foo", 5));
	}

	@Test
	public void shouldUpdateExistingDocument() throws Exception {
		VersionedClass one = new VersionedClass(id, "foo", 0);
		template.save(one);

		template.save(new VersionedClass(id, "foo1", one.version));

		VersionedClass value = template.findById(id, VersionedClass.class);
		assertThat(value.version).isEqualTo(2);
		assertThat(value.field).isEqualTo("foo1");
	}

	@Test
	public void shouldSetVersionWhenSavingTheSameDocument() throws Exception {
		VersionedClass one = new VersionedClass(id, "foo");
		template.save(one);
		template.save(one);
		template.save(one);

		assertThat(one.version).isEqualTo(3);
	}

	@Test
	public void shouldUpdateAlreadyExistingDocument() throws Exception {
		AtomicLong counter = new AtomicLong();
		int numberOfConcurrentSaves = 5;

		VersionedClass initial = new VersionedClass(id, "value-0");
		template.save(initial);
		assertThat(initial.version).isEqualTo(1);

		AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            boolean saved = false;
            while(!saved) {
                long counterValue = counter.incrementAndGet();
                VersionedClass messageData = template.findById(id, VersionedClass.class);
                messageData.field = "value-" + counterValue;
                try {
                    template.save(messageData);
                    saved = true;
                } catch (OptimisticLockingFailureException e) {
                }
            }
            return null;
        });

		VersionedClass actual = template.findById(id, VersionedClass.class);

		assertThat(actual.field).isNotEqualTo(initial.field);
		assertThat(actual.version).isNotEqualTo(initial.version);
		assertThat(actual.version).isEqualTo(initial.version + numberOfConcurrentSaves);
	}

	@Test
	public void shouldSaveOnlyFirstDocumentAndNextAttemptsShouldFailWithOptimisticLockingException() throws Exception {
		AtomicLong counter = new AtomicLong();
		AtomicLong optimisticLockCounter = new AtomicLong();
		int numberOfConcurrentSaves = 5;

		AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
			long counterValue = counter.incrementAndGet();
			String data = "value-" + counterValue;
			VersionedClass messageData = new VersionedClass(id, data);
            try {
                template.save(messageData);
            } catch (OptimisticLockingFailureException e) {
                optimisticLockCounter.incrementAndGet();
            }
            return null;
        });

		assertThat(optimisticLockCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
	}

	@Test
	public void findById_shouldSetVersionEqualToNumberOfModifications() throws Exception {
		template.insert(new VersionedClass(id, "foobar"));
		template.update(new VersionedClass(id, "foobar1"));
		template.update(new VersionedClass(id, "foobar2"));

		Record raw = client.get(new Policy(), new Key(info.getNamespace(), "versioned-set", id));
		assertThat(raw.generation).isEqualTo(3);
		VersionedClass actual = template.findById(id, VersionedClass.class);
		assertThat(actual.version).isEqualTo(3);
	}

	@Test
	public void shouldSaveMultipleTimeDocumentWithoutVersion() throws Exception {
		CustomCollectionClass one = new CustomCollectionClass(id, "numbers");

		template.save(one);
		template.save(one);

		assertThat(template.findById(id, CustomCollectionClass.class)).isEqualTo(one);
	}

	@Test
	public void shouldUpdateDocumentDataWithoutVersion() throws Exception {
		CustomCollectionClass first = new CustomCollectionClass(id, "numbers");
		CustomCollectionClass second = new CustomCollectionClass(id, "hot dog");

		template.save(first);
		template.save(second);

		assertThat(template.findById(id, CustomCollectionClass.class)).isEqualTo(second);
	}

	@Test
	public void findById_shouldReturnNullForNonExistingKey() throws Exception {
		Person one = template.findById("person-non-existing-key", Person.class);

		assertThat(one).isNull();
	}

	@Test
	public void find_shouldReturnEmptyResultForQueryWithNoResults() throws Exception {
		template.createIndex(Person.class, "Person_age_index", "age", IndexType.NUMERIC );
		Query<?> query = new Query<Object>(
				Criteria.where("age").is(-10, "age"));

		Iterable<Person> it = template.find(query, Person.class);

		int count = 0;
		for (Person person : it) {
			count++;
		}
		assertThat(count).isZero();
	}

	@Test
	public void shouldInsertAndFindWithCustomCollectionSet() throws Exception {
		CustomCollectionClass initial = new CustomCollectionClass(id, "data0");
		template.insert(initial);

		Record record = client.get(new Policy(), new Key(info.getNamespace(), "custom-set", id));

		assertThat(record.getString("data")).isEqualTo("data0");
		assertThat(template.findById(id, CustomCollectionClass.class)).isEqualTo(initial);
	}

	@Test
	public void insertsSimpleEntityCorrectly() {
		Person person = new Person("Person-01","Oliver");
		person.setAge(25);
		template.insert(person);

		Person person1 =  template.findById("Person-01", Person.class);
		assertThat(person1).isEqualTo(person);
	}

	@Test
	public void findbyIdFail() {
		Person person = new Person("Person-01","Oliver");
		person.setAge(25);
		template.insert(person);

		Person person1 =  template.findById("Person", Person.class);
		assertThat(person1).isNull();
	}

	@Test (expected = DuplicateKeyException.class)
	public void throwsExceptionForDuplicateIds() {
		Person person = new Person("Person-02","Amol");
		person.setAge(28);

		template.insert(person);
		template.insert(person);
	}

	@Test (expected = DuplicateKeyException.class)
	public void rejectsDuplicateIdInInsertAll() {
		Person person = new Person("Biff-01", "Amol");
		person.setAge(28);

		List<Person> records = new ArrayList<Person>();
		records.add(person);
		records.add(person);

		template.insertAll(records);
	}

	@Test
	public void findMultipleFiltersQualifierOnly(){
		template.createIndex(Person.class, "Person_firstName_index", "firstName",IndexType.STRING );

		Person personSven01 = new Person("Sven-01","ZLastName",25);
		Person personSven02 = new Person("Sven-02","QLastName",21);
		Person personSven03 = new Person("Sven-03","ALastName",24);
		Person personSven04 = new Person("Sven-04","WLastName",25);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		Qualifier qual1 = new Qualifier("age", FilterOperation.EQ, Value.get(25));
		Iterable<Person> it = template.findAllUsingQuery(Person.class, null, qual1);
		int count = 0;
		for (Person person : it){
			count++;
		}
		assertThat(count).isEqualTo(2);
	}

	@Test
	public void findMultipleFiltersFilterAndQualifier(){
		template.createIndex(Person.class, "Person_firstName_index", "firstName",IndexType.STRING );

		Person personSven01 = new Person("Sven-01","John",25);
		Person personSven02 = new Person("Sven-02","John",21);
		Person personSven03 = new Person("Sven-03","John",24);
		Person personSven04 = new Person("Sven-04","WFirstName",25);
		Person personSven05 = new Person("Sven-05","ZFirstName",25);
		Person personSven06 = new Person("Sven-06","QFirstName",21);
		Person personSven07 = new Person("Sven-07","AFirstName",24);
		Person personSven08 = new Person("Sven-08","John",25);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);
		template.insert(personSven05);
		template.insert(personSven06);
		template.insert(personSven07);
		template.insert(personSven08);

		Filter filter = Filter.equal("firstName", "John");
		Qualifier qual1 = new Qualifier("age", FilterOperation.EQ, Value.get(25));
		Iterable<Person> it = template.findAllUsingQuery(Person.class, filter, qual1);
		int count = 0;
		for (Person person : it){
			count++;
		}
		assertThat(count).isEqualTo(2);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void checkIndexingString() {
		template.createIndex(Person.class, "Person_firstName_index", "firstName",IndexType.STRING );

		Person personSven01 = new Person("Sven-01","ZLastName",25);
		Person personSven02 = new Person("Sven-02","QLastName",21);
		Person personSven03 = new Person("Sven-03","ALastName",24);
		Person personSven04 = new Person("Sven-04","WLastName",25);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		Query query = new Query(Criteria.where("firstName").is("ALastName","firstName"));

		Iterable<Person> it = template.find(query, Person.class);
		int count = 0;
		Person firstPerson = null;
		for (Person person : it){
			firstPerson = person;
			count++;
		}
		assertThat(count).isEqualTo(1);
		assertThat(firstPerson).isEqualTo(personSven03);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void checkIndexingViaNumeric() {
		template.createIndex(Person.class, "Person_age_index", "age",IndexType.NUMERIC );

		Person personSven01 = new Person("Sven-01","ZLastName",25);
		Person personSven02 = new Person("Sven-02","QLastName",21);
		Person personSven03 = new Person("Sven-03","ALastName",24);
		Person personSven04 = new Person("Sven-04","WLastName",35);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		Query query = new Query(Criteria.where("age").is(35,"age"));

		Iterable<Person> it = template.find(query, Person.class);
		int count = 0;
		Person firstPerson = null;
		for (Person person : it){
			firstPerson = person;
			count++;
		}
		assertThat(count).isEqualTo(1);
		assertThat(firstPerson).isEqualTo(personSven04);
	}


	@Test(expected = DataRetrievalFailureException.class)
	public void shouldThrowExceptionOnUpdateForNonexistingKey(){
		template.update(new Person("Sven-06","svenfirstName",11));
	}

	@Test
	public void testUpdateSuccess(){
		Person person = new Person("Sven-04","WLastName",11);
		template.insert(person);

		template.update(person);

		Person result = template.findById("Sven-04", Person.class);

		assertThat(result.getAge()).isEqualTo(11);
	}

	@Test
	public void testSimpleDeleteByObject(){
		Person personSven01 = new Person("Sven-01","ZLastName",25);
		Person personSven02 = new Person("Sven-02","QLastName",21);
		Person personSven03 = new Person("Sven-03","ALastName",24);
		Person personSven04 = new Person("Sven-04","WLastName",35);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		boolean deleted = template.delete(personSven02);
		assertThat(deleted).isTrue();

		Person result = template.findById("Sven-02", Person.class);
		assertThat(result).isNull();
	}

	@Test
	public void testSimpleDeleteById(){
		Person personSven01 = new Person("Sven-01","ZLastName",25);
		Person personSven02 = new Person("Sven-02","QLastName",21);
		Person personSven03 = new Person("Sven-03","ALastName",24);
		Person personSven04 = new Person("Sven-04","WLastName",35);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		boolean deleted = template.delete("Sven-02", Person.class);
		assertThat(deleted).isTrue();

		Person result = template.findById("Sven-02", Person.class);
		assertThat(result).isNull();
	}

	@Test
	public void StoreAndRetrieveDate(){
		template.createIndex(Person.class, "Person_dateOfBirth_index", "dateOfBirth",IndexType.STRING );

		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		Date birthday1 = null;

		Person personSven01 = new Person("Sven-01","ZLastName",25);
		Person personSven02 = new Person("Sven-02","QLastName",50);
		Person personSven03 = new Person("Sven-03","ALastName",24);
		Person personSven04 = new Person("Sven-04","WLastName",25);
		try {
			birthday1 = formatter.parse("8-Apr-1965");
			personSven01.setDateOfBirth(formatter.parse("7-Jun-1903"));
			personSven02.setDateOfBirth(birthday1);
			personSven03.setDateOfBirth(formatter.parse("7-Jan-1957"));
			personSven04.setDateOfBirth(formatter.parse("7-Oct-2000"));
		} catch (Exception e) {
			// TODO: handle exception
		}

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		Person findDate = template.findById("Sven-02", Person.class);

		assertThat(findDate.getDateOfBirth()).isEqualTo(birthday1);
	}

	@Test
	public void StoreAndRetrieveMap(){
		Person personSven01 = new Person("Sven-01","ZLastName",25);
		Person personSven02 = new Person("Sven-02","QLastName",50);
		Person personSven03 = new Person("Sven-03","ALastName",24);
		Person personSven04 = new Person("Sven-04","WLastName",25);
		Map<String, String> map = new HashMap<String, String>();
		map.put("key", "value");
			personSven02.setMap(map);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		Person findDate = template.findById("Sven-02", Person.class);

		assertThat(findDate.getMap()).isEqualTo(map);
	}

	@Test
	public void StoreAndRetrieveList(){
		Person personSven01 = new Person("Sven-01", "ZLastName", 25);
		Person personSven02 = new Person("Sven-02", "QLastName", 50);
		Person personSven03 = new Person("Sven-03", "ALastName", 24);
		Person personSven04 = new Person("Sven-04", "WLastName", 25);
		Map<String, String> map = new HashMap<String, String>();
		map.put("key1", "value1");
		map.put("key2", "value2");
		map.put("key3", "value3");
		personSven02.setMap(map);
		ArrayList<String> list = new ArrayList<String>();
		list.add("string1");
		list.add("string2");
		list.add("string3");
		personSven02.setList(list);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		Person findDate = template.findById("Sven-02", Person.class);

		assertThat(findDate.getMap()).isEqualTo(map);
		assertThat(findDate.getList()).isEqualTo(list);
	}

	@Test
	public void TestAddToList() {
		Person personSven01 = new Person("Sven-01", "ZLastName", 25);
		Person personSven02 = new Person("Sven-02", "QLastName", 50);
		Person personSven03 = new Person("Sven-03", "ALastName", 24);
		Person personSven04 = new Person("Sven-04", "WLastName", 25);
		Map<String, String> map = new HashMap<String, String>();
		map.put("key1", "value1");
		map.put("key2", "value2");
		map.put("key3", "value3");
		personSven02.setMap(map);
		ArrayList<String> list = new ArrayList<String>();
		list.add("string1");
		list.add("string2");
		list.add("string3");
		personSven02.setList(list);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		Person personWithList = template.findById("Sven-02", Person.class);
		personWithList.getList().add("Added something new");
		template.update(personWithList);
		Person personWithList2 = template.findById("Sven-02", Person.class);

		assertThat(personWithList2).isEqualTo(personWithList);
		assertThat(personWithList2.getList()).hasSize(4);
	}

	@Test
	public void TestAddToMap() {

		Person personSven01 = new Person("Sven-01", "ZLastName", 25);
		Person personSven02 = new Person("Sven-02", "QLastName", 50);
		Person personSven03 = new Person("Sven-03", "ALastName", 24);
		Person personSven04 = new Person("Sven-04", "WLastName", 25);
		Map<String, String> map = new HashMap<String, String>();
		map.put("key1", "value1");
		map.put("key2", "value2");
		map.put("key3", "value3");
		personSven02.setMap(map);
		ArrayList<String> list = new ArrayList<String>();
		list.add("string1");
		list.add("string2");
		list.add("string3");
		personSven02.setList(list);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		Person personWithList = template.findById("Sven-02", Person.class);
		personWithList.getMap().put("key4","Added something new");
		template.update(personWithList);
		Person personWithList2 = template.findById("Sven-02", Person.class);

		assertThat(personWithList2).isEqualTo(personWithList);
		assertThat(personWithList2.getMap()).hasSize(4);
		assertThat(personWithList2.getMap().get("key4")).isEqualTo("Added something new");

	}

	@Test (expected = NullPointerException.class)
	public void removingNullIsANoOp() {
		template.delete(null);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void countsDocumentsCorrectly() {
		template.createIndex(Person.class, "Person_firstName_index", "firstName",IndexType.STRING );

		Person personSven01 = new Person("Sven-01", "ZLastName", 25);
		Person personSven02 = new Person("Sven-02", "QLastName", 50);
		Person personSven03 = new Person("Sven-03", "ALastName", 24);
		Person personSven04 = new Person("Sven-04", "WLastName", 25);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		Query query = new Query(Criteria.where("firstName").is("ALastName","firstName"));
		int qCount = template.count(query, Person.class);
		assertThat(qCount).isEqualTo(1);
		assertThat(template.count(Person.class)).isEqualTo(4L);
	}

	@Test(expected = IllegalArgumentException.class)
	public void countRejectsNullEntityClass() {
		template.count(null, (Class<?>) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullObjectToBeSaved() {
		template.save(null);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void executesExistsCorrectly() {
		template.createIndex(Person.class, "Person_firstName_index", "firstName",IndexType.STRING );

		Person personSven01 = new Person("Sven-01", "ZLastName", 25);
		Person personSven02 = new Person("Sven-02", "QLastName", 50);
		Person personSven03 = new Person("Sven-03", "ALastName", 24);
		Person personSven04 = new Person("Sven-04", "WLastName", 25);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		Query queryExist = new Query(Criteria.where("firstName").is("ALastName","firstName"));
		Query queryNotExist = new Query(Criteria.where("firstName").is("Biff","firstName"));
		assertThat(template.exists(queryExist, Person.class)).isTrue();
		assertThat(template.exists(queryNotExist, Person.class)).isFalse();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void updateConsidersMappingAnnotations() {
		template.createIndex(Person.class, "Person_firstName_index", "firstName",IndexType.STRING );

		Person personSven01 = new Person("Sven-01", "ZLastName", 25);
		personSven01.setEmailAddress("old@mail.com");

		template.insert(personSven01);

		Person personWithMail = template.findById("Sven-01", Person.class);
		assertThat(personWithMail.getEmailAddress()).isEqualTo("old@mail.com");

		personWithMail.setEmailAddress("new@mail.com");

		template.update(personWithMail);

		Query query = new Query(Criteria.where("firstName").is(personWithMail.getFirstName(),"firstName"));
		Iterable<Person> it = template.find(query, Person.class);

		int count = 0;
		Person firstPerson = null;
		for (Person person : it){
			firstPerson = person;
			count++;
		}
		assertThat(count).isEqualTo(1);
		assertThat(firstPerson).isEqualTo(personWithMail);
		assertThat(personWithMail.getEmailAddress()).isEqualTo("new@mail.com");
	}

	@Test
	public void shouldAdd() {
		Person one = Person.builder().id(id).age(25).build();
		template.insert(one);

		Person updated = template.add(one, "age", 1);

		assertThat(updated.getAge()).isEqualTo(26);
	}

	@Test
	public void shouldAppend() throws Exception {
		Person one = Person.builder().id(id).firstName("Nas").build();
		template.insert(one);

		Person appended = template.append(one, "firstName", "tya");

		assertThat(appended).isEqualTo(Person.builder().id(id).firstName("Nastya").build());
		assertThat(appended.getFirstName()).isEqualTo("Nastya");
		assertThat(template.findById(id, Person.class).getFirstName()).isEqualTo("Nastya");
	}

	@Test
	public void shouldAppendMultipleFields() throws Exception {
		Person one = Person.builder().id(id).firstName("Nas").emailAddress("nastya@").build();
		template.insert(one);

		Map<String, String> toBeUpdated = new HashMap<>();
		toBeUpdated.put("firstName", "tya");
		toBeUpdated.put("email", "gmail.com");
		Person appended = template.append(one, toBeUpdated);

		assertThat(appended.getFirstName()).isEqualTo("Nastya");
		assertThat(appended.getEmailAddress()).isEqualTo("nastya@gmail.com");
		Person actual = template.findById(id, Person.class);
		assertThat(actual.getFirstName()).isEqualTo("Nastya");
		assertThat(actual.getEmailAddress()).isEqualTo("nastya@gmail.com");
	}

	@Test
	public void shouldPrepend() throws Exception {
		Person one = Person.builder().id(id).firstName("tya").build();
		template.insert(one);

		Person appended = template.prepend(one, "firstName", "Nas");

		assertThat(appended.getFirstName()).isEqualTo("Nastya");
		assertThat(template.findById(id, Person.class).getFirstName()).isEqualTo("Nastya");
	}

	@Test
	public void shouldPrependMultipleFields() throws Exception {
		Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
		template.insert(one);

		Map<String, String> toBeUpdated = new HashMap<>();
		toBeUpdated.put("firstName", "Nas");
		toBeUpdated.put("email", "nastya@");
		Person appended = template.prepend(one, toBeUpdated);

		assertThat(appended.getFirstName()).isEqualTo("Nastya");
		assertThat(appended.getEmailAddress()).isEqualTo("nastya@gmail.com");
		Person actual = template.findById(id, Person.class);
		assertThat(actual.getFirstName()).isEqualTo("Nastya");
		assertThat(actual.getEmailAddress()).isEqualTo("nastya@gmail.com");

	}

	@Test
	public void shouldPersistWithCustomWritePolicy() throws Exception {
		CustomCollectionClass initial = new CustomCollectionClass(id, "data");

		WritePolicy writePolicy = new WritePolicy();
		writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

		template.persist(initial, writePolicy);

		CustomCollectionClass actual = template.findById(id, CustomCollectionClass.class);
		assertThat(actual).isEqualTo(initial);
	}

	@Test(expected = DataRetrievalFailureException.class)
	public void shouldNotPersistWithCustomWritePolicy() throws Exception {
		CustomCollectionClass initial = new CustomCollectionClass(id, "data");

		WritePolicy writePolicy = new WritePolicy();
		writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

		template.persist(initial, writePolicy);
	}

	@Test(expected = DuplicateKeyException.class)
	public void shouldTranslateException() {
		Key key = new Key(template.getNamespace(), "shouldTranslateException", "shouldTranslateException");
		Bin bin = new Bin("bin_name", "bin_value");

		template.getAerospikeClient().add(null, key, bin);
		template.execute(() -> {
			AerospikeClient client = template.getAerospikeClient();
			WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
			writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

			client.add(writePolicy, key, bin);
			return true;
		});
	}

	@Test
	public void exists_shouldReturnTrueIfValueIsPresent() {
		Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
		template.insert(one);

		assertThat(template.exists(id, Person.class)).isTrue();
	}

	@Test
	public void exists_shouldReturnFalseIfValueIsAbsent() {
		assertThat(template.exists(id, Person.class)).isFalse();
	}

	@Test
	public void deleteById_shouldReturnFalseIfValueIsAbsent() {
		assertThat(template.delete(id, Person.class)).isFalse();
	}

	@Test
	public void deleteByObject_shouldReturnFalseIfValueIsAbsent() {
		Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
		assertThat(template.delete(one)).isFalse();
	}

	@Test
	public void findByIds_shouldFindExisting() {
		Person firstPerson = Person.builder().id(nextId()).firstName("first").emailAddress("gmail.com").build();
		template.save(firstPerson);

		Person secondPerson = Person.builder().id(nextId()).firstName("second").emailAddress("gmail.com").build();
		template.save(secondPerson);

		List<String> ids = Arrays.asList(nextId(), firstPerson.getId(), secondPerson.getId());

		List<Person> actual = template.findByIds(ids, Person.class);
		assertThat(actual).containsExactly(firstPerson, secondPerson);
	}

	@Test
	public void findByIds_shouldReturnEmptyList() {
		List<Person> actual = template.findByIds(Collections.emptyList(), Person.class);
		assertThat(actual).isEmpty();
	}
}
