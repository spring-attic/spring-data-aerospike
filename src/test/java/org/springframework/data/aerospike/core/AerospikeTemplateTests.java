/**
 * 
 */
package org.springframework.data.aerospike.core;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.data.aerospike.repository.BaseRepositoriesIntegrationTests;
import org.springframework.data.aerospike.repository.query.Criteria;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.helper.query.Qualifier;
import com.aerospike.helper.query.Qualifier.FilterOperation;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class AerospikeTemplateTests extends BaseRepositoriesIntegrationTests {

	@Autowired AerospikeTemplate template;
	@Autowired AerospikeClient client;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		cleanDb();
	}

	private void cleanDb() {
		template.delete(Person.class);

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		cleanDb();
	}

	@Test
	public void insertsSimpleEntityCorrectly() {
		Person person = new Person("Person-01","Oliver");
		person.setAge(25);
		template.insert(person);

		Person person1 =  template.findById("Person-01", Person.class);
		assertThat(person1 , is(person));
	}

	@Test
	public void findbyIdFail() {
		Person person = new Person("Person-01","Oliver");
		person.setAge(25);
		template.insert(person);

		Person person1 =  template.findById("Person", Person.class);
		assertNull(person1);
	}

	@Test (expected = DataIntegrityViolationException.class)
	public void throwsExceptionForDuplicateIds() {
		Person person = new Person("Person-02","Amol");
		person.setAge(28);

		template.insert(person);
		template.insert(person);
	}

	@Test (expected = DataIntegrityViolationException.class)
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
		Person firstPerson = null;
		for (Person person : it){
			firstPerson = person;
			System.out.print(firstPerson+"\n");
			count++;
		}
		Assert.assertEquals(2, count);
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
		Person firstPerson = null;
		for (Person person : it){
			firstPerson = person;
			System.out.print(firstPerson+"\n");
			count++;
		}
		Assert.assertEquals(2, count);
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
			System.out.print(person+"\n");
			count++;
		}
		Assert.assertEquals(1, count);
		Assert.assertEquals(firstPerson, personSven03);
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
			System.out.print(person+"\n");
			count++;
		}
		Assert.assertEquals(1, count);
		Assert.assertEquals(firstPerson, personSven04);
	}


	@Test  (expected = RecoverableDataAccessException.class)
	public void testUpdateFailure(){
		Person personSven01 = new Person("Sven-01","ZLastName",25);
		Person personSven02 = new Person("Sven-02","QLastName",21);
		Person personSven03 = new Person("Sven-03","ALastName",24);
		Person personSven04 = new Person("Sven-04","WLastName",35);
		Person personSven05 = new Person("Sven-05","svenfirstName",11);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		template.update("Sven-06", personSven05);
	}

	@Test 
	public void testUpdateSuccess(){
		Person personSven01 = new Person("Sven-01","ZLastName",25);
		Person personSven02 = new Person("Sven-02","QLastName",21);
		Person personSven03 = new Person("Sven-03","ALastName",24);
		Person personSven04 = new Person("Sven-04","WLastName",35);
		Person personSven05 = new Person("Sven-04","WLastName",11);

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		template.update("Sven-04", personSven05);

		Person result = template.findById("Sven-04", Person.class);

		Assert.assertEquals(result.getAge(), 11);
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

		template.delete(personSven02);

		Person result = template.findById("Sven-02", Person.class);
		assertNull(result);
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

		template.delete("Sven-02", Person.class);

		Person result = template.findById("Sven-02", Person.class);
		assertNull(result);
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

		Assert.assertEquals(findDate.getDateOfBirth(), birthday1);
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

		Assert.assertEquals(findDate.getMap(), map);
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

		Assert.assertEquals(findDate.getMap(), map);
		Assert.assertEquals(findDate.getList(), list);
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
		Person personWithList2 = template.findOne("Sven-02", Person.class);

		Assert.assertEquals(personWithList2, personWithList);
		Assert.assertEquals(personWithList2.getList().size(), 4);
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
		Person personWithList2 = template.findOne("Sven-02", Person.class);

		Assert.assertEquals(personWithList2, personWithList);
		Assert.assertEquals(personWithList2.getMap().size(), 4);
		Assert.assertEquals(personWithList2.getMap().get("key4"), "Added something new");

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
		assertThat(qCount, is(1));
		assertThat(template.count(Person.class), is(4L));
	}

	@Test(expected = IllegalArgumentException.class)
	public void countRejectsNullEntityClass() {
		template.count(null, (Class<?>) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullObjectToBeSaved() {
		template.save("",null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullTypeObjectToBeSaved() {
		template.save("",null,null,null);
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
		assertThat(template.exists(queryExist, Person.class),is(true));
		assertThat(template.exists(queryNotExist, Person.class),is(false));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void updateConsidersMappingAnnotations() {
		template.createIndex(Person.class, "Person_firstName_index", "firstName",IndexType.STRING );

		Person personSven01 = new Person("Sven-01", "ZLastName", 25);
		Person personSven02 = new Person("Sven-02", "QLastName", 50);
		Person personSven03 = new Person("Sven-03", "ALastName", 24);
		Person personSven04 = new Person("Sven-04", "WLastName", 25);
		personSven01.setEmailAddress("old@mail.com");

		template.insert(personSven01);
		template.insert(personSven02);
		template.insert(personSven03);
		template.insert(personSven04);

		Person personWithMail = template.findById("Sven-01", Person.class);
		assertThat(personWithMail.getEmailAddress(), is("old@mail.com"));

		personWithMail.setEmailAddress("new@mail.com");

		template.update(personWithMail);

		Query query = new Query(Criteria.where("firstName").is(personWithMail.getFirstName(),"firstName"));
		Iterable<Person> it = template.find(query, Person.class);

		int count = 0;
		Person firstPerson = null;
		for (Person person : it){
			firstPerson = person;
			System.out.print(person+"\n");
			count++;
		}
		Assert.assertEquals(1, count);
		Assert.assertEquals(firstPerson, personWithMail);
		assertThat(personWithMail.getEmailAddress(), is("new@mail.com"));
	}

	@Test
	public void TestAdd() {
		Person personSven01 = new Person("Sven-01", "ZLastName", 25);

		template.insert(personSven01);
		template.add(personSven01, "age", 1);

		//clean up
		template.delete(personSven01);
		Assert.assertEquals(26, personSven01.getAge());
	}
}
