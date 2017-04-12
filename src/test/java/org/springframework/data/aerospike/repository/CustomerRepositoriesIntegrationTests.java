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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.CustomerRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Oliver Gierke
 */
public class CustomerRepositoriesIntegrationTests extends BaseRepositoriesIntegrationTests {

	@Autowired CustomerRepository repository;

	@Test
	public void testCreate() {
		repository.save(new Customer("dave-001", "Dave", "Matthews"));
	}

	@Test
	public void testExists() {
		repository.save(new Customer("dave-001", "Dave", "Matthews"));
		boolean exists = repository.exists("dave-001");
		assertTrue(exists);
	}

	@Test
	public void testDelete() {
		repository.delete(new Customer("dave-001", "Dave", "Matthews"));
	}

	@Test
	public void testReadById() {
		Customer customer = repository.save(new Customer("dave-001", "Dave", "Matthews"));
		Customer findById = repository.findOne("dave-001");

		assertNotNull(findById);
		assertEquals(customer.getLastname(), findById.getLastname());
		assertEquals(customer.getFirstname(), findById.getFirstname());
	}

}
