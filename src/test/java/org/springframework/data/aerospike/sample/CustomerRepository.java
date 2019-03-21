/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.sample;

import java.util.List;

import org.springframework.data.aerospike.repository.AerospikeRepository;

/**
 * @author Oliver Gierke
 */
public interface CustomerRepository extends AerospikeRepository<Customer, String> {

	List<Customer> findByLastname(String lastname);
	List<Customer> findByFirstname(String firstname);
	List<Customer> findCustomerByFirstname(String firstname);
	List<Customer> findCustomerByAgeBetween(Integer from,Integer to);
	List<Customer> findCustomerByFirstnameStartingWithIgnoreCase(String firstname);

	List<Customer> findCustomerByLastnameOrderByFirstnameAsc(String lastname);
}
