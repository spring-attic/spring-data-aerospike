/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.aerospike.log4j;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Calendar;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;

/**
 * Integration tests for {@link AerospikeLog4jAppender}.
 * 
 * @authorPeter Milne
 */
public class AerospikeLog4jAppenderIntegrationTests {

	static final String NAME = AerospikeLog4jAppenderIntegrationTests.class.getName();

	Logger log = Logger.getLogger(NAME);
	AerospikeClient client;
	String namespace;
	String set;
	

	@Before
	public void setUp() throws Exception {

		this.client = new AerospikeClient("localhost", 3000);

	}

	@Test
	public void testLogging() {

		log.debug("DEBUG message");
		log.info("INFO message");
		log.warn("WARN message");
		log.error("ERROR message");


	}

	@Test
	public void testProperties() {

		MDC.put("property", "one");
		log.debug("DEBUG message");
	}
}
