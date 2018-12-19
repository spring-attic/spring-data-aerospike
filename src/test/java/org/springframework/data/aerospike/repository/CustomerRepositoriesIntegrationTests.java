/*******************************************************************************
 * Copyright (c) 2018 the original author or authors.
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
import java.util.Optional;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;

import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.CustomerRepository;

import jersey.repackaged.com.google.common.collect.Lists;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;


/**
 * @author Oliver Gierke
 */
public class CustomerRepositoriesIntegrationTests extends BaseIntegrationTests {

	@Autowired CustomerRepository repository;

	@Test
	public void testCreate() {
		repository.save(new Customer("dave-001", "Dave", "Matthews"));
	}

	@Test
	public void testExists() {
		repository.save(new Customer("dave-001", "Dave", "Matthews"));
		boolean exists = repository.existsById("dave-001");
		assertTrue(exists);
	}

	@Test
	public void testDelete() {
		repository.delete(new Customer("dave-001", "Dave", "Matthews"));
	}

	@Test
	public void testReadById() {
		Customer customer = repository.save(new Customer("dave-001", "Dave", "Matthews"));
		Optional<Customer> findById = repository.findById("dave-001");

		assertThat(findById).hasValueSatisfying(actual -> {
			assertThat(actual.getLastname()).isEqualTo(customer.getLastname());
			assertThat(actual.getFirstname()).isEqualTo(customer.getFirstname());
		});
	}

	@Test
	public void testFindAllByIDs(){
		repository.save(new Customer("dave-001", "Dave", "AMatthews"));
		repository.save(new Customer("dave-002", "Dave", "BMatthews"));
		repository.save(new Customer("dave-003", "Dave", "CMatthews"));
		repository.save(new Customer("dave-004", "Dave", "DMatthews"));
		Iterable<Customer> customers = repository.findAllById(Arrays.asList("dave-001", "dave-004"));
		assertEquals(Lists.newArrayList(customers).size(), 2);
	}
}
