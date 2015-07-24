/*******************************************************************************
 * Copyright (c) 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *  	
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package org.springframework.data.aerospike.repository;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.repository.Person.Sex;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.query.IndexType;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
public abstract class AbstractPersonRepositoryIntegrationTests {
	
	@Autowired protected PersonRepository repository;

	@Autowired AerospikeOperations operations;
	
	@Autowired AerospikeClient client;
	
	static int count = 0;

	Person dave, oliver, carter, boyd, stefan, leroi, alicia;
	
	List<Person> all,allKeyLess;
	
	@Before
	public void setUp() throws InterruptedException {
		

		repository.deleteAll();

		dave = new Person("Dave-01","Dave", "Matthews", 42);
		oliver = new Person("Oliver-01","Oliver August", "Matthews", 4);
		carter = new Person("Carter-01", "Carter", "Beauford", 49);
		Thread.sleep(10);
		boyd = new Person("Boyd-01","Boyd", "Tinsley", 45);
		stefan = new Person("Stefan-01","Stefan", "Lessard", 34);
		leroi = new Person("Leroi-01","Leroi", "Moore", 41);
		alicia = new Person("Alicia-01","Alicia", "Keys", 30, Sex.FEMALE);
		
		repository.createIndex(Person.class,"last_name_index", "lastname", IndexType.STRING);
		repository.createIndex(Person.class,"first_name_index", "firstname", IndexType.STRING);
		repository.createIndex(Person.class,"person_age_index", "age", IndexType.NUMERIC);





		all = (List<Person>) repository.save(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia));
		

	}
	

	@Test
	public void testSetup(){
		
		Assert.isInstanceOf(Person.class, dave);
	}
	
	@Test
	public void findsPersonById() throws Exception {

		Person person = repository.findOne(dave.getId().toString());
		Assert.isInstanceOf(Person.class, person);
		assertThat(repository.findOne(dave.getId().toString()), is(dave));
	}
	@Test
	public void findsAllMusicians() throws Exception {
		List<Person> result = (List<Person>) repository.findAll();
		assertThat(result.size(), is(all.size()));
		assertThat(result.containsAll(all), is(true));
	}
	

	
	@Test
	public void findsAllWithGivenIds() {

		Iterable<Person> result = repository.findAll(Arrays.asList(dave.id, boyd.id));
		assertThat(result, hasItems(dave, boyd));
		assertThat(result, not(hasItems(oliver, carter, stefan, leroi, alicia)));
	}
	
	@Test
	public void deletesPersonCorrectly() throws Exception {

		repository.delete(dave);

		List<Person> result = (List<Person>) repository.findAll();

		assertThat(result.size(), is(all.size() - 1));
		assertThat(result, not(hasItem(dave)));
	}
	
	@Test
	public void deletesPersonByIdCorrectly() {

		repository.delete(dave.getId().toString());

		List<Person> result = (List<Person>) repository.findAll();

		assertThat(result.size(), is(all.size() - 1));
		assertThat(result, not(hasItem(dave)));
	}
	
	@Test
	public void findsPersonsByLastname() throws Exception {

		List<Person> result = repository.findByLastname("Beauford");
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(carter));
	}
	
	@Test
	public void findsPersonsByFirstname() {

		List<Person> result = repository.findByFirstname("Leroi");
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(leroi));
		assertThat(result.get(0).getAge(), is(41));
	}
	
	@Ignore("Aerospike Query does not support like") @Test
	public void findsPersonsByFirstnameLike() throws Exception {

		List<Person> result = repository.findByFirstnameLike("Bo*");
		assertThat(result.size(), is(1));
		assertThat(result, hasItem(boyd));
	}
	
	@Test
	public void findsPagedPersons() throws Exception {

		Page<Person> result = repository.findAll(new PageRequest(1, 2, Direction.ASC, "lastname", "firstname"));
		assertThat(result.isFirst(), is(false));
		assertThat(result.isLast(), is(false));
		assertThat(result, hasItems(leroi, stefan));
	}
	@Ignore("Aerospike Query does not support like")  @Test
	public void executesPagedFinderCorrectly() throws Exception {

//		Page<Person> page = repository.findByLastnameLike("*a*",
//				new PageRequest(0, 2, Direction.ASC, "lastname", "firstname"));
//		assertThat(page.isFirst(), is(true));
//		assertThat(page.isLast(), is(false));
//		assertThat(page.getNumberOfElements(), is(2));
//		assertThat(page, hasItems(carter, stefan));
	}
	
	@Test
	public void findsPersonInAgeRangeCorrectly() throws Exception {

		List<Person> result = repository.findByAgeBetween(40, 45);
		assertThat(result.size(), is(3));
		assertThat(result, hasItems(dave, leroi,boyd));
	}

//	@Test
//	public void findsPersonByShippingAddressesCorrectly() throws Exception {
//
//		Address address = new Address("Foo Street 1", "C0123", "Bar");
//		dave.setShippingAddresses(new HashSet<Address>(asList(address)));
//
//		repository.save(dave);
//		assertThat(repository.findByShippingAddresses(address), is(dave));
//	}
	
}
