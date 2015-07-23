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

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.repository.Person.Sex;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;

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
	public void deletesPersonCorrectly() throws Exception {

		repository.delete(dave);

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

}
