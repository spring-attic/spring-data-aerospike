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

import java.util.HashMap;
import java.util.Map;

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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Integration tests for {@link AerospikeTemplate}.
 * 
 * @author Oliver Gierke
 * 
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
	@Autowired AerospikeClient client;
	
	@Before
	public void cleanUp(){
		
		client.delete(null, new Key("test", "Customer", "dave-001"));
		client.delete(null, new Key("test", "Customer", "dave-002"));
		client.delete(null, new Key("test", "Customer", "dave-003"));
	}
	
	@Test
	public void testInsertWithKey(){
		Customer customer = new Customer("dave-001", "Dave", "Matthews");
		template.insert("dave-001", customer);
		Record result = client.get(null, new Key("test", "Customer", "dave-001"));
		Assert.assertEquals("Dave", result.getString("firstname"));
		Assert.assertEquals("Matthews", result.getString("lastname"));
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
		Record result = client.get(null, new Key("test", "Customer", "dave-002"));
		Assert.assertEquals("Dave", result.getString("firstname"));
		Assert.assertEquals("Matthews", result.getString("lastname"));
	}
	@Test
	public void testFindByKey(){
		client.put(null, new Key("test", "Customer", "dave-003"), new Bin("firstname", "Dave"), 
				new Bin ("lastname", "Matthews"));
		Customer result = template.findById("dave-003", Customer.class);
		Assert.assertEquals("Matthews", result.getLastname());
		Assert.assertEquals("Dave", result.getFirstname());
		
	}
	@Test
	public void testSingleIncrement(){
		Customer customer = new Customer("dave-002", "Dave", "Matthews");
		template.insert(customer);
		template.add(customer, "age", 1);
		Record result = client.get(null, new Key("test", "Customer", "dave-002"), "age");
		Assert.assertEquals(1, result.getInt("age"));
	}
	@Test
	public void testMultipleIncrement(){
		Customer customer = new Customer("dave-002", "Dave", "Matthews");
		template.insert(customer);
		Map<String, Long> values = new HashMap<String, Long>();
		values.put("age", 1L);
		values.put("waist", 32L);
		
		template.add(customer, values);
		Record result = client.get(null, new Key("test", "Customer", "dave-002"), "age", "waist");
		Assert.assertEquals(1, result.getInt("age"));
		Assert.assertEquals(32, result.getInt("waist"));
	}
	@Test
	public void testDelete(){
		client.put(null, new Key("test", "Customer", "dave-001"), new Bin("firstname", "Dave"), 
				new Bin ("lastname", "Matthews"));
		Customer customer = new Customer("dave-001", "Dave", "Matthews");
		template.delete(customer);
		if ( client.exists(null, new Key("test", "Customer", "dave-001")))
		Assert.fail("dave-001 was not deleted");
	}

}


