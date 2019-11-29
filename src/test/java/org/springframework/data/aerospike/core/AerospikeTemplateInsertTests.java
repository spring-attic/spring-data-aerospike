/*
 * Copyright 2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.SampleClasses;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AerospikeTemplateInsertTests extends BaseIntegrationTests {

	private String id;

	@Before
	public void setUp() {
		this.id = nextId();
	}

	@Test
	public void insertsAndFindsWithCustomCollectionSet() {
		SampleClasses.CustomCollectionClass initial = new SampleClasses.CustomCollectionClass(id, "data0");
		template.insert(initial);

		Record record = client.get(new Policy(), new Key(getNameSpace(), "custom-set", id));

		assertThat(record.getString("data")).isEqualTo("data0");
		assertThat(template.findById(id, SampleClasses.CustomCollectionClass.class)).isEqualTo(initial);
	}

	@Test
	public void insertsDocumentWithListMapDateStringLongValues() {
		Map<String, String> map = Collections.singletonMap("k", "v");
		Person friend = new Person(nextId(), "Anna", 43);
		List<String> list = Arrays.asList("a", "b", "c");
		String email = "dave@gmail.com";
		Date dateOfBirth = new Date();
		Person customer = new Person(id, "Dave", 45, map, friend, true, dateOfBirth, list, email);

		template.insert(customer);

		Person actual = template.findById(id, Person.class);
		assertThat(actual.getId()).isEqualTo(id);
		assertThat(actual.getAge()).isEqualTo(45);
		assertThat(actual.getFirstName()).isEqualTo("Dave");
		assertThat(actual.getMap()).isEqualTo(map);
		assertThat(actual.getFriend()).isEqualToIgnoringGivenFields(friend, "id");//metadata fields are not saved for internal structures
		assertThat(actual.isActive()).isTrue();
		assertThat(actual.getDateOfBirth()).isEqualTo(dateOfBirth);
		assertThat(actual.getList()).isEqualTo(list);
		assertThat(actual.getEmailAddress()).isEqualTo(email);
	}

	@Test
	public void insertsAndFindsDocumentWithByteArrayField() {
		SampleClasses.DocumentWithByteArray document = new SampleClasses.DocumentWithByteArray(id, new byte[]{1, 0, 0, 1, 1, 1, 0, 0});

		template.insert(document);

		SampleClasses.DocumentWithByteArray result = template.findById(id, SampleClasses.DocumentWithByteArray.class);
		assertThat(result).isEqualTo(document);
	}

	@Test
	public void insertsDocumentWithNullFields() {
		SampleClasses.VersionedClass document = new SampleClasses.VersionedClass(id, null);

		template.insert(document);

		assertThat(document.getField()).isNull();
	}

	@Test
	public void insertsDocumentWithZeroVersionIfThereIsNoDocumentWithSameKey() {
		SampleClasses.VersionedClass document = new SampleClasses.VersionedClass(id, "any", 0);

		template.insert(document);

		assertThat(document.getVersion()).isEqualTo(1);
	}

	@Test
	public void insertsDocumentWithVersionGreaterThanZeroIfThereIsNoDocumentWithSameKey() {
		SampleClasses.VersionedClass document = new SampleClasses.VersionedClass(id, "any", 5);

		template.insert(document);

		assertThat(document.getVersion()).isEqualTo(1);
	}

	@Test
	public void throwsExceptionForDuplicateId() {
		Person person = new Person(id, "Amol", 28);

		template.insert(person);
		assertThatThrownBy(() -> template.insert(person))
				.isInstanceOf(DuplicateKeyException.class);
	}

	@Test
	public void throwsExceptionForDuplicateIdForVersionedDocument() {
		SampleClasses.VersionedClass document = new SampleClasses.VersionedClass(id, "any", 5);

		template.insert(document);
		assertThatThrownBy(() -> template.insert(document))
				.isInstanceOf(DuplicateKeyException.class);
	}

	@Test
	public void insertsOnlyFirstDocumentAndNextAttemptsShouldFailWithDuplicateKeyExceptionForVersionedDocument() throws Exception {
		AtomicLong counter = new AtomicLong();
		AtomicLong duplicateKeyCounter = new AtomicLong();
		int numberOfConcurrentSaves = 5;

		AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
			long counterValue = counter.incrementAndGet();
			String data = "value-" + counterValue;
			try {
				template.insert(new SampleClasses.VersionedClass(id, data));
			} catch (DuplicateKeyException e) {
				duplicateKeyCounter.incrementAndGet();
			}
		});

		assertThat(duplicateKeyCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
	}

	@Test
	public void insertsOnlyFirstDocumentAndNextAttemptsShouldFailWithDuplicateKeyExceptionForNonVersionedDocument() throws Exception {
		AtomicLong counter = new AtomicLong();
		AtomicLong duplicateKeyCounter = new AtomicLong();
		int numberOfConcurrentSaves = 5;

		AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
			long counterValue = counter.incrementAndGet();
			String data = "value-" + counterValue;
			try {
				template.insert(new Person(id, data, 28));
			} catch (DuplicateKeyException e) {
				duplicateKeyCounter.incrementAndGet();
			}
		});

		assertThat(duplicateKeyCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);

	}
}
