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
package org.springframework.data.aerospike.core;

import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests for {@link AerospikeTemplate}.
 * 
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration

public class AerospikeTemplateIntegrationTests {

	@Configuration
	@EnableAerospikeRepositories(basePackageClasses = AerospikeTemplate.class)
	static class Config extends TestConfiguration {

	}
	//@Autowired AerospikeOperations operations;

	@Autowired AerospikeTemplate template;
	
	@Before
	public void cleanUp(){
		template.delete("dave-001", Customer.class);
		template.delete("dave-002", Customer.class);
		template.delete("dave-003", Customer.class);
	}
	
	@Test
	public void testInsertWithKey(){
		Customer customer = new Customer("dave-001", "Dave", "Matthews");
		template.insert("dave-001", customer);
	}
	@Test
	public void testUpdateWithKey(){
		Customer customer = new Customer("dave-001", "Dave", "Matthews");
		template.insert("dave-001", customer);
		customer.setLastName(customer.getLastname() + "xx");
		template.update("dave-001", customer);
		customer = template.findById("dave-001", Customer.class);
		Assert.assertEquals("Matthewsxx", customer.getLastname());
	}
	@Test
	public void testInsert(){
		Customer customer = new Customer("dave-002", "Dave", "Matthews");
		template.insert(customer);
	}
	@Test
	public void testFindById(){
		Customer customer = new Customer("dave-003", "Dave", "Matthews");
		template.insert("dave-003", customer);
		Customer result = template.findById("dave-003", Customer.class);
		Assert.assertEquals("Matthews", result.getLastname());
		Assert.assertEquals("Dave", result.getFirstname());
		
	}
	@Test
	public void testIncrement(){
		Customer customer = new Customer("dave-002", "Dave", "Matthews");
		template.insert(customer);
		template.add(customer, "age", 1);
		customer = template.findById("dave-002", Customer.class);
		long age = customer.getAge();
		Assert.assertEquals(1, age);
	}
}


