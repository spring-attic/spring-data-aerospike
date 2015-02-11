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
package org.springframework.data.aerospike.convert;

import org.junit.Test;
import org.springframework.data.aerospike.sample.AerospikeDataTester;
import org.springframework.data.aerospike.sample.Customer;

import com.aerospike.client.Key;

/**
 * Integration tests for {@link MappingAerospikeConverter}.
 * 
 * @author Oliver Gierke
 */
public class MappingAerospikeConverterIntegrationTests {

	MappingAerospikeConverter converter = new MappingAerospikeConverter();

	/**
	 * @see DATAAERO-1
	 */
	@Test
	public void convertsIdAndSimpleProperties() {

		Customer customer = new Customer("Dave", "Matthews");
		AerospikeData data = AerospikeData.forWrite("test");

		converter.write(customer, data);

		AerospikeDataTester tester = new AerospikeDataTester(data);

		tester.assertBinHasValue("firstname", "Dave");
		tester.assertBinHasValue("lastname", "Matthews");
		tester.assertHasKey(new Key("test", Customer.class.getSimpleName(), customer.getId().toString()));
	}
}
