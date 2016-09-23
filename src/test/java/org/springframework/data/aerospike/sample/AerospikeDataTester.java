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
package org.springframework.data.aerospike.sample;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import junit.framework.AssertionFailedError;

import org.springframework.data.aerospike.convert.AerospikeData;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

/**
 * @author Oliver Gierke
 */
public class AerospikeDataTester {

	private final AerospikeData data;

	/**
	 * @param data
	 */
	public AerospikeDataTester(AerospikeData data) {
		this.data = data;
	}

	public void assertHasKey(Key key) {
		assertThat(data.getKey(), is(key));
	}

	public void assertBinHasValue(String name, Object value) {
		for (Bin bin : data.getBins()) {
			if (bin.name.equals(name)) {
				assertThat(bin.value.getObject(), is(value));
				return;
			}
		}

		throw new AssertionFailedError(String.format("Couldn't find bin with name %s!", name));
	}
}
