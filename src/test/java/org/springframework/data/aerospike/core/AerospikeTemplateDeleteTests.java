package org.springframework.data.aerospike.core;

import com.aerospike.client.policy.GenerationPolicy;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeTemplateDeleteTests extends BaseIntegrationTests {

	private String id;

	@Before
	public void setUp() {
		this.id = nextId();
	}

	@Test
	public void deleteByObject_ignoresDocumentVersionEvenIfDefaultGenerationPolicyIsSet() {
		GenerationPolicy initialGenerationPolicy = client.writePolicyDefault.generationPolicy;
		client.writePolicyDefault.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		try {
			VersionedClass initialDocument = new VersionedClass(id, "a");
			template.insert(initialDocument);
			template.update(new VersionedClass(id, "b", initialDocument.version));

			boolean deleted = template.delete(initialDocument);
			assertThat(deleted).isTrue();
		} finally {
			client.writePolicyDefault.generationPolicy = initialGenerationPolicy;
		}
	}

	@Test
	public void deleteByObject_ignoresVersionEvenIfDefaultGenerationPolicyIsSet() {
		GenerationPolicy initialGenerationPolicy = client.writePolicyDefault.generationPolicy;
		client.writePolicyDefault.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		try {
			Person initialDocument = new Person(id, "a");
			template.insert(initialDocument);
			template.update(new Person(id, "b"));

			boolean deleted = template.delete(initialDocument);
			assertThat(deleted).isTrue();
		} finally {
			client.writePolicyDefault.generationPolicy = initialGenerationPolicy;
		}
	}

	@Test
	public void deleteByObject_deletesDocument() {
		Person document = new Person(id, "QLastName", 21);
		template.insert(document);

		boolean deleted = template.delete(document);
		assertThat(deleted).isTrue();

		Person result = template.findById(id, Person.class);
		assertThat(result).isNull();
	}

	@Test
	public void deleteById_deletesDocument() {
		Person document = new Person(id, "QLastName", 21);
		template.insert(document);

		boolean deleted = template.delete(id, Person.class);
		assertThat(deleted).isTrue();

		Person result = template.findById(id, Person.class);
		assertThat(result).isNull();
	}

	@Test
	public void deleteById_returnsFalseIfValueIsAbsent() {
		assertThat(template.delete(id, Person.class)).isFalse();
	}

	@Test
	public void deleteByObject_returnsFalseIfValueIsAbsent() {
		Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
		assertThat(template.delete(one)).isFalse();
	}
}
