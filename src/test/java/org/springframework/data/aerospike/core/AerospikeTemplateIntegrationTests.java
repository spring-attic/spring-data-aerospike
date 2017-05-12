/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.policy.WritePolicy;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.repository.*;
import org.springframework.data.aerospike.repository.Person;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.ObjectUtils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

/**
 * Integration tests for {@link AerospikeTemplate}.
 * 
 * @author Oliver Gierke
 * 
 */
public class AerospikeTemplateIntegrationTests extends BaseRepositoriesIntegrationTests {
	
	protected static final String SET_NAME_PERSON = "Person";

	@Autowired AerospikeTemplate template;
	@Autowired AerospikeClient client;

	DefaultRepositoryMetadata  repositoryMetaData =  new DefaultRepositoryMetadata(ContactRepository.class);
	private WritePolicy policy = getWritePolicy();

	@Before
	public void cleanUp(){
		ScanPolicy scanPolicy = new ScanPolicy();
		scanPolicy.includeBinData = false;
		final AtomicLong count = new AtomicLong();
		client.scanAll(	scanPolicy, getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON,  new ScanCallback() {
				@Override
				public void scanCallback(Key key, Record record)
						throws AerospikeException {

					if (client.delete(null, key)) 
						count.addAndGet(1);
						/*
						 * after 10,000 records delete, return print the count.
						 */
						if (count.get() % 10000 == 0){
							System.out.println("Deleted "+ count.get());
						}

				}
		}, new String[] {});
		System.out.println("Deleted "+ count + " records from set " + AerospikeTemplateIntegrationTests.SET_NAME_PERSON);
	}

	@Test
	public void testUpdate(){
		Person customer = new Person("dave-001", "Dave", "Matthews");
		template.insert(customer);
		String newLastName = customer.getLastname() + "xx";
		customer.setLastname(newLastName);
		template.update(customer);
		customer = template.findById("dave-001", Person.class);
		Assert.assertEquals("Matthewsxx", customer.getLastname());
	}

	@Test
	public void testInsert(){
		Person customer = new Person("dave-002", "Dave", "Matthews");
		template.insert(customer);
		Record result = client.get(null, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"));
		Assert.assertEquals("Dave", result.getString("firstname"));
		Assert.assertEquals("Matthews", result.getString("lastname"));
	}

	@Test
	public void testFindByKey(){
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-003"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		Person result = template.findById("dave-003", Person.class);
		Assert.assertEquals("Matthews", result.getLastname());
		Assert.assertEquals("Dave", result.getFirstname());
	}

	@Test
	public void testSingleIncrement(){
		Person customer = new Person("dave-002", "Dave", "Matthews");
		template.insert(customer);
		customer = template.add(customer, "age", 1);
		Record result = client.get(null, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"), "age");
		Assert.assertEquals(1, result.getInt("age"));
		Assert.assertEquals((Integer)result.getInt("age"), customer.getAge());
	}

	@Test
	public void testMultipleIncrement(){
		Person customer = new Person("dave-002", "Dave", "Matthews");
		template.insert(customer);
		Map<String, Long> values = new HashMap<String, Long>();
		values.put("age", 1L);

		customer = template.add(customer, values);
		Record result = client.get(null, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"), "age", "waist");
		Assert.assertEquals(1, result.getInt("age"));
		Assert.assertEquals((Integer)result.getInt("age"), customer.getAge());
	}

	@Test
	public void testDelete(){
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-001"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		Person customer = new Person("dave-001", "Dave", "Matthews");
		template.delete(customer);
		if ( client.exists(null, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-001")))
			Assert.fail("dave-001 was not deleted");
	}

	@Test
	public void testFindAll(){
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-001"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-003"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-004"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-005"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-006"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-007"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-008"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-009"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-010"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		List<Person> list = template.findAll(Person.class);
		Assert.assertEquals(10, list.size());
	}

	@SuppressWarnings("unchecked")
	private <T> Query<T> createQueryForMethodWithArgs(String methodName, Object... args)
			throws NoSuchMethodException, SecurityException {

		Class<?>[] argTypes = new Class<?>[args.length];
		if (!ObjectUtils.isEmpty(args)) {

			for (int i = 0; i < args.length; i++) {
				argTypes[i] = args[i].getClass();
			}
		}

		Method method = PersonRepository.class.getMethod(methodName, argTypes);

		PartTree partTree = new PartTree(method.getName(), Person.class);
		AerospikeQueryCreator creator = new AerospikeQueryCreator(partTree, new ParametersParameterAccessor(new QueryMethod(method,repositoryMetaData, new SpelAwareProxyProjectionFactory()).getParameters(), args));

		Query<T> q = (Query<T>) creator.createQuery();

		return q;
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testFindWithFilterEqual() throws NoSuchMethodException, Exception{
		IndexTask task = client.createIndex(null, getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "first_name_index", "firstname", IndexType.STRING);
		task.waitTillComplete();

		WritePolicy policy = getWritePolicy();
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-001"), new Bin("firstname", "Dave"), new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"), new Bin("firstname", "Dave"), new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-003"), new Bin("firstname", "Dave"), new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-004"), new Bin("firstname", "Dave"), new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-005"), new Bin("firstname", "Dave"), new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-006"), new Bin("firstname", "Dave"), new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-007"), new Bin("firstname", "Dave"), new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-008"), new Bin("firstname", "Dave"), new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-009"), new Bin("firstname", "Dave"), new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-010"), new Bin("firstname", "Dave"), new Bin ("lastname", "Matthews"));

		Query query = createQueryForMethodWithArgs("findPersonByFirstname", "Dave");

		Iterable<Person> it = template.find(query, Person.class);
		int count = 0;
		for (Person customer : it){
			System.out.print(customer+"\n");
			count++;
		}
		Assert.assertEquals(10, count);
	}

	@SuppressWarnings("rawtypes")
	@Test 
	public void testFindWithFilterEqualOrderBy() throws NoSuchMethodException, Exception{
		IndexTask task = client.createIndex(null, getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "age_index", "age", IndexType.NUMERIC);
		task.waitTillComplete();
		IndexTask task2 = client.createIndex(null, getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "last_name_index", "lastname", IndexType.STRING);
		task2.waitTillComplete();

		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-001"), new Bin(
				"firstname", "Jean"), new Bin("lastname", "Matthews"), new Bin(
				"age", 21));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"), new Bin(
				"firstname", "Ashley"), new Bin("lastname", "Matthews"), new Bin(
				"age", 22));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-003"), new Bin(
				"firstname", "Beatrice"), new Bin("lastname", "Matthews"), new Bin(
				"age", 23));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-004"), new Bin(
				"firstname", "Dave"), new Bin("lastname", "Matthews"), new Bin(
				"age", 24));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-005"), new Bin(
				"firstname", "Zaipper"), new Bin("lastname", "Matthews"), new Bin(
				"age", 25));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-006"), new Bin(
				"firstname", "knowlen"), new Bin("lastname", "Matthews"), new Bin(
				"age", 26));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-007"), new Bin(
				"firstname", "Xylophone"), new Bin("lastname", "Matthews"), new Bin(
				"age", 27));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-008"), new Bin(
				"firstname", "Mitch"), new Bin("lastname", "Matthews"), new Bin(
				"age", 28));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-009"), new Bin(
				"firstname", "Alister"), new Bin("lastname", "Matthews"), new Bin(
				"age", 29));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-010"), new Bin(
				"firstname", "Aabbbt"), new Bin("lastname", "Matthews"), new Bin(
				"age", 30));

		Query query = createQueryForMethodWithArgs("findByLastnameOrderByFirstnameAsc","Matthews");

		Iterable<Person> it = template.find(query, Person.class);
		int count = 0;
		for (Person person : it){
			System.out.print(person+"\n");
			count++;
		}
		Assert.assertEquals(10, count);
	}

	@SuppressWarnings("rawtypes")
	@Test 
	public void testFindWithFilterEqualOrderByDesc() throws NoSuchMethodException, Exception{
		IndexTask task = client.createIndex(null, getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "age_index", "age", IndexType.NUMERIC);
		task.waitTillComplete();
		IndexTask task2 = client.createIndex(null, getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "last_name_index", "lastname", IndexType.STRING);
		task2.waitTillComplete();

		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-001"), new Bin(
				"firstname", "Jean"), new Bin("lastname", "Matthews"), new Bin(
				"age", 21));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"), new Bin(
				"firstname", "Ashley"), new Bin("lastname", "Matthews"), new Bin(
				"age", 22));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-003"), new Bin(
				"firstname", "Beatrice"), new Bin("lastname", "Matthews"), new Bin(
				"age", 23));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-004"), new Bin(
				"firstname", "Dave"), new Bin("lastname", "Matthews"), new Bin(
				"age", 24));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-005"), new Bin(
				"firstname", "Zaipper"), new Bin("lastname", "Matthews"), new Bin(
				"age", 25));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-006"), new Bin(
				"firstname", "knowlen"), new Bin("lastname", "Matthews"), new Bin(
				"age", 26));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-007"), new Bin(
				"firstname", "Xylophone"), new Bin("lastname", "Matthews"), new Bin(
				"age", 27));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-008"), new Bin(
				"firstname", "Mitch"), new Bin("lastname", "Matthews"), new Bin(
				"age", 28));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-009"), new Bin(
				"firstname", "Alister"), new Bin("lastname", "Matthews"), new Bin(
				"age", 29));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-010"), new Bin(
				"firstname", "Aabbbt"), new Bin("lastname", "Matthews"), new Bin(
				"age", 30));
		Object [] args = {"Matthews"};
		Query query = createQueryForMethodWithArgs("findByLastnameOrderByFirstnameDesc",args);

		Iterable<Person> it = template.find(query, Person.class);
		int count = 0;
		for (Person person : it){
			System.out.print(person+"\n");
			count++;
		}
		Assert.assertEquals(10, count);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testFindWithFilterRange() throws NoSuchMethodException, Exception{
		IndexTask task = client.createIndex(null, getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "age_index", "age", IndexType.NUMERIC);
		task.waitTillComplete();

		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-001"), new Bin(
				"firstname", "Dave01"), new Bin("lastname", "Matthews"), new Bin(
				"age", 21));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"), new Bin(
				"firstname", "Dave02"), new Bin("lastname", "Matthews"), new Bin(
				"age", 22));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-003"), new Bin(
				"firstname", "Dave03"), new Bin("lastname", "Matthews"), new Bin(
				"age", 23));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-004"), new Bin(
				"firstname", "Dave04"), new Bin("lastname", "Matthews"), new Bin(
				"age", 24));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-005"), new Bin(
				"firstname", "Dave05"), new Bin("lastname", "Matthews"), new Bin(
				"age", 25));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-006"), new Bin(
				"firstname", "Dave06"), new Bin("lastname", "Matthews"), new Bin(
				"age", 26));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-007"), new Bin(
				"firstname", "Dave07"), new Bin("lastname", "Matthews"), new Bin(
				"age", 27));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-008"), new Bin(
				"firstname", "Dave08"), new Bin("lastname", "Matthews"), new Bin(
				"age", 28));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-009"), new Bin(
				"firstname", "Dave09"), new Bin("lastname", "Matthews"), new Bin(
				"age", 29));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-010"), new Bin(
				"firstname", "Dave10"), new Bin("lastname", "Matthews"), new Bin(
				"age", 30));
		
		Query query = createQueryForMethodWithArgs("findCustomerByAgeBetween", 25,30);

		Iterable<Person> it = template.find(query, Person.class);
		int count = 0;
		for (Person person : it){
			System.out.print(person.toString()+"\n");
			count++;
		}
		Assert.assertEquals(6, count);
	}

	@Test
	public void testFindWithStatement(){
		IndexTask task = client.createIndex(null, getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "first_name_index", "firstname", IndexType.STRING);
		task.waitTillComplete();

		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-001"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-002"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-003"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-004"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-005"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-006"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-007"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-008"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-009"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));
		client.put(policy, new Key(getNameSpace(), AerospikeTemplateIntegrationTests.SET_NAME_PERSON, "dave-010"), new Bin("firstname", "Dave"),
				new Bin ("lastname", "Matthews"));

		Statement aerospikeQuery = new Statement();
		String[] bins = {"firstname","lastname"}; //fields we want retrieved
		aerospikeQuery.setNamespace(getNameSpace()); // Database
		aerospikeQuery.setSetName(AerospikeTemplateIntegrationTests.SET_NAME_PERSON); //Table
		aerospikeQuery.setBinNames(bins);
		aerospikeQuery.setFilters(Filter.equal("firstname","Dave")); //Query

		RecordSet rs =  client.query(null, aerospikeQuery);
		int count = 0;
		while (rs.next()) {
			Record r = rs.getRecord();
			System.out.print(r.getValue("lastname")+ ","+r.getValue("firstname"));
			count++;
		}

		Assert.assertEquals(10, count);
	}

	private WritePolicy getWritePolicy() {
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		return policy;
	}

}

