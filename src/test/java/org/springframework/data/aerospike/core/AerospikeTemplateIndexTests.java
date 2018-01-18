package org.springframework.data.aerospike.core;

import com.aerospike.client.Value;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.helper.query.Qualifier;
import org.junit.After;
import org.junit.Test;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.repository.query.Criteria;
import org.springframework.data.aerospike.repository.query.Query;

import java.text.SimpleDateFormat;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class AerospikeTemplateIndexTests extends BaseIntegrationTests {

	@After
	public void setUp() {
		cleanDb();
	}

	@Test
	public void shouldCreateAndDeleteIndex() throws Exception {
		assertThat(template.indexExists("check-index-1")).isFalse();
		assertThat(template.indexExists("check-index-2")).isFalse();

		template.createIndex(Person.class, "check-index-1", "active", IndexType.NUMERIC);
		template.createIndex(Person.class, "check-index-2", "id", IndexType.NUMERIC);

		assertThat(template.indexExists("check-index-1")).isTrue();
		assertThat(template.indexExists("check-index-2")).isTrue();

		template.deleteIndex(Person.class, "check-index-1");
		template.deleteIndex(Person.class, "check-index-2");

		await().untilAsserted(() -> assertThat(template.indexExists("check-index-1")).isFalse());
		await().untilAsserted(() -> assertThat(template.indexExists("check-index-2")).isFalse());
	}

	@Test
	public void checkIndexingString() {
		template.createIndex(Person.class, "Person_firstName_index", "firstName", IndexType.STRING);
		Person p1 = new Person(nextId(), "ZLastName", 25);
		Person p2 = new Person(nextId(), "QLastName", 21);
		Person p3 = new Person(nextId(), "ALastName", 24);
		Person p4 = new Person(nextId(), "WLastName", 25);
		template.insertAll(asList(p1, p2, p3, p4));

		Query query = new Query(Criteria.where("firstName").is("ALastName", "firstName"));
		Iterable<Person> it = template.find(query, Person.class);

		assertThat(it).containsOnly(p3);
	}

	@Test
	public void checkIndexingViaNumeric() {
		template.createIndex(Person.class, "Person_age_index", "age", IndexType.NUMERIC);
		Person p1 = new Person(nextId(), "ZLastName", 25);
		Person p2 = new Person(nextId(), "QLastName", 21);
		Person p3 = new Person(nextId(), "ALastName", 24);
		Person p4 = new Person(nextId(), "WLastName", 35);
		template.insertAll(asList(p1, p2, p3, p4));

		Query query = new Query(Criteria.where("age").is(35, "age"));
		Iterable<Person> it = template.find(query, Person.class);

		assertThat(it).containsOnly(p4);
	}

	@Test
	public void countsDocumentsCorrectly() {
		template.createIndex(Person.class, "Person_firstName_index", "firstName", IndexType.STRING);
		Person p1 = new Person(nextId(), "ZLastName", 25);
		Person p2 = new Person(nextId(), "QLastName", 50);
		Person p3 = new Person(nextId(), "ALastName", 24);
		Person p4 = new Person(nextId(), "WLastName", 25);
		template.insertAll(asList(p1, p2, p3, p4));

		Query query = new Query(Criteria.where("firstName").is("ALastName", "firstName"));

		assertThat(template.count(query, Person.class)).isEqualTo(1);
		assertThat(template.count(Person.class)).isEqualTo(4L);
	}

	@Test
	public void executesExistsCorrectly() {
		template.createIndex(Person.class, "Person_firstName_index", "firstName", IndexType.STRING);
		Person p1 = new Person(nextId(), "ZLastName", 25);
		Person p2 = new Person(nextId(), "QLastName", 50);
		Person p3 = new Person(nextId(), "ALastName", 24);
		Person p4 = new Person(nextId(), "WLastName", 25);
		template.insertAll(asList(p1, p2, p3, p4));

		Query queryExist = new Query(Criteria.where("firstName").is("ALastName", "firstName"));
		Query queryNotExist = new Query(Criteria.where("firstName").is("Biff", "firstName"));

		assertThat(template.exists(queryExist, Person.class)).isTrue();
		assertThat(template.exists(queryNotExist, Person.class)).isFalse();
	}

	@Test
	public void find_shouldReturnEmptyResultForQueryWithNoResults() throws Exception {
		template.createIndex(Person.class, "Person_age_index", "age", IndexType.NUMERIC);
		Query<?> query = new Query<Object>(
				Criteria.where("age").is(-10, "age"));

		Iterable<Person> it = template.find(query, Person.class);

		assertThat(it).isEmpty();
	}

	@Test
	public void findMultipleFiltersFilterAndQualifier() {
		template.createIndex(Person.class, "Person_firstName_index", "firstName", IndexType.STRING);
		Person p1 = new Person(nextId(), "John", 25);
		Person p2 = new Person(nextId(), "John", 21);
		Person p3 = new Person(nextId(), "John", 24);
		Person p4 = new Person(nextId(), "WFirstName", 25);
		Person p5 = new Person(nextId(), "ZFirstName", 25);
		Person p6 = new Person(nextId(), "QFirstName", 21);
		Person p7 = new Person(nextId(), "AFirstName", 24);
		Person p8 = new Person(nextId(), "John", 25);
		template.insertAll(asList(p1, p2, p3, p4, p5, p6, p7, p8));

		Filter filter = Filter.equal("firstName", "John");
		Qualifier qual1 = new Qualifier("age", Qualifier.FilterOperation.EQ, Value.get(25));
		Iterable<Person> it = template.findAllUsingQuery(Person.class, filter, qual1);

		assertThat(it).containsOnly(p1, p8);
	}

	@Test
	public void findMultipleFiltersQualifierOnly() {
		template.createIndex(Person.class, "Person_firstName_index", "firstName", IndexType.STRING);
		Person p1 = new Person(nextId(), "ZLastName", 25);
		Person p2 = new Person(nextId(), "QLastName", 21);
		Person p3 = new Person(nextId(), "ALastName", 24);
		Person p4 = new Person(nextId(), "WLastName", 25);
		template.insertAll(asList(p1, p2, p3, p4));

		Qualifier qual1 = new Qualifier("age", Qualifier.FilterOperation.EQ, Value.get(25));
		Iterable<Person> it = template.findAllUsingQuery(Person.class, null, qual1);

		assertThat(it).hasSize(2);
	}

	@Test
	public void StoreAndRetrieveDate() throws Exception {
		template.createIndex(Person.class, "Person_dateOfBirth_index", "dateOfBirth", IndexType.STRING);
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		Person p1 = new Person(nextId(), "ZLastName", 25);
		Person p2 = new Person(nextId(), "QLastName", 50);
		Person p3 = new Person(nextId(), "ALastName", 24);
		Person p4 = new Person(nextId(), "WLastName", 25);
		p2.setDateOfBirth(formatter.parse("8-Apr-1965"));
		p3.setDateOfBirth(formatter.parse("7-Jan-1957"));
		p4.setDateOfBirth(formatter.parse("7-Oct-2000"));
		template.insertAll(asList(p1, p2, p3, p4));

		Person findDate = template.findById(p2.getId(), Person.class);

		//TODO: execute query with search by date of birth here and check it
		assertThat(findDate.getDateOfBirth()).isEqualTo(p2.getDateOfBirth());
	}

	@Test
	public void updateConsidersMappingAnnotations() {
		template.createIndex(Person.class, "Person_firstName_index", "firstName", IndexType.STRING);

		Person p1 = new Person(nextId(), "ZLastName", 25);
		p1.setEmailAddress("old@mail.com");

		template.insert(p1);

		Person personWithMail = template.findById(p1.getId(), Person.class);
		assertThat(personWithMail.getEmailAddress()).isEqualTo("old@mail.com");

		personWithMail.setEmailAddress("new@mail.com");

		template.update(personWithMail);

		Query query = new Query(Criteria.where("firstName").is(personWithMail.getFirstName(), "firstName"));
		Iterable<Person> it = template.find(query, Person.class);

		assertThat(it).containsOnly(personWithMail);
	}

}
