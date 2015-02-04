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
package org.springframework.data.aerospike.repository;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.core.TestConfiguration;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.CustomerRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Oliver Gierke
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@Ignore
// Ignored until we have the infrastructure set up to actually run the tests
public class CustomerRepositoriesIntegrationTests {

	@Configuration
	@EnableAerospikeRepositories(basePackageClasses = CustomerRepository.class)
	static class Config extends TestConfiguration {

	}

	@Autowired CustomerRepository repository;

	@Test
	public void testname() {

		Customer customer = repository.save(new Customer("Dave", "Matthews"));
		List<Customer> findByLastname = repository.findByLastname("Matthews");

		assertThat(findByLastname, hasSize(1));
		assertThat(findByLastname, hasItem(customer));
	}

}
