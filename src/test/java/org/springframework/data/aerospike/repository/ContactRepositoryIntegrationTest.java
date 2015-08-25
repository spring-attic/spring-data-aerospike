/**
 * 
 */
package org.springframework.data.aerospike.repository;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.sample.CustomerRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
/**
 * Integration tests for {@link ContactRepository}. Mostly related to mapping inheritance.
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfig.class)

public class ContactRepositoryIntegrationTest {

	
	
	@Autowired
	ContactRepository repository;

	@Test
	public void readsAndWritesContactCorrectly() {
		Person person = new Person("1","Oliver", "Gierke");
		Contact result = repository.save(person);
		Contact findById = repository.findOne(result.getId().toString());
		assertNotNull(findById);
		assertTrue(findById instanceof Person);
	}

}
