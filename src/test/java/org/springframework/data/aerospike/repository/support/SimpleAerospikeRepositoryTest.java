/**
 * 
 */
package org.springframework.data.aerospike.repository.support;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.springframework.context.ApplicationContext;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.core.Person;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;

import com.aerospike.client.query.IndexType;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class SimpleAerospikeRepositoryTest {

	@Mock ApplicationContext applicationContext;
	@Mock AerospikeTemplate template;
	@Mock RepositoryInformation repositoryInformation;
	@Mock AerospikeConverter converter;
	@Mock EntityInformation<?, String> metadata;
	@Mock AerospikeRepositoryFactory aerospikeRepositoryFactoryMock;
	@Mock AerospikePersistentEntity<?> entity;
	@Mock AerospikeOperations operations;

	SimpleAerospikeRepository<?, String> simpleAerospikeRepository = null;
	Person testPerson = null;

	@Rule public ExpectedException exception = ExpectedException.none();

	/**
	 * @throws java.lang.Exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Before
	public void setUp() throws Exception {
		simpleAerospikeRepository = new SimpleAerospikeRepository(metadata, operations);
		Mockito.<Class<?>>when(metadata.getJavaType()).thenReturn(Person.class);
		testPerson = new Person("21", "Jean");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#findOne(java.io.Serializable)}.
	 */
	@Test
	public void testFindOne() {
		//Mockito.when(operations.findOne("21", Person.class)).thenReturn(new Person("21", "Jean"));


		Mockito.when(operations.findById("21",Person.class)).thenReturn(testPerson);
		Person person = (Person) simpleAerospikeRepository.findOne("21");
		assertThat(person.getFirstName(), org.hamcrest.Matchers.equalToIgnoringCase("Jean"));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#save(java.lang.Object)}.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testSaveS() {
		SimpleAerospikeRepository<Person, String> aerospikeRepository = (SimpleAerospikeRepository<Person, String>) mock(SimpleAerospikeRepository.class);
		when(aerospikeRepository.save(Mockito.any(Person.class))).thenAnswer(new Answer<Person>(){

			@Override
			public Person answer(InvocationOnMock invocation) throws Throwable {
				Person arg = (Person) invocation.getArguments()[0];
				return arg;
			}
		});

		Person myPerson = aerospikeRepository.save(testPerson);

		assertThat(testPerson,is(myPerson));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#save(java.lang.Iterable)}.
	 */
	@SuppressWarnings({ "unchecked", "serial" })
	@Test
	public void testSaveIterableOfS() {
		SimpleAerospikeRepository<Person, String> aerospikeRepository = (SimpleAerospikeRepository<Person, String>) mock(SimpleAerospikeRepository.class);
		List<Person> persons = new ArrayList<Person>(){{
			add(new Person("one", "Jean", 21));
			add(new Person("two", "Jean2", 22));
			add(new Person("three", "Jean3", 23));
		}};
		when(aerospikeRepository.save(persons)).then(
			new Answer<List<Person>>() {
				@Override
				public List<Person> answer(InvocationOnMock invocation)
						throws Throwable {
					List<Person> arg = (List<Person>) invocation.getArguments()[0];
					return SimpleAerospikeRepository.convertIterableToList(arg);
				}
			});
		
		aerospikeRepository.save(persons);
		assertThat(persons, is(containsInAnyOrder(new Person("one", "Jean", 21),new Person("two", "Jean2", 22),new Person("three", "Jean3", 23))));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#delete(java.lang.Object)}.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testDeleteT() {
		SimpleAerospikeRepository<Person, String> aerospikeRepository = (SimpleAerospikeRepository<Person, String>) mock(SimpleAerospikeRepository.class);
		doNothing().when(aerospikeRepository).delete(testPerson);
		aerospikeRepository.delete(testPerson);
	}


	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#findAll(org.springframework.data.domain.Sort)}.
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	@Test
	public void testFindAllSort() {
		List<Person> persons = new ArrayList<Person>(){{
			add(new Person("one", "Jean", 21));
			add(new Person("two", "Jean2", 22));
			add(new Person("three", "Jean3", 23));
		}};

		when(operations.findAll(new Sort(Sort.Direction.ASC,"biff"),Person.class)).thenReturn(persons);
		List<Person> fetchList = (List<Person>) simpleAerospikeRepository.findAll(new Sort(Sort.Direction.ASC,"biff"));
		assertThat(fetchList, containsInAnyOrder(new Person("one", "Jean", 21),new Person("two", "Jean2", 22),new Person("three", "Jean3", 23)));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#findAll(org.springframework.data.domain.Pageable)}.
	 * @param <T>
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	@Test
	public <T> void testFindAllPageable() {
		List<Person> persons = new ArrayList<Person>(){{
			add(new Person("one", "Jean", 21));
			add(new Person("two", "Jean2", 22));
			add(new Person("three", "Jean3", 23));
		}};
		Page<T> page = new PageImpl<T>((List<T>) IterableConverter.toList(persons), new PageRequest(0, 2),5);

		//doReturn(persons).when(operations).findInRange(anyInt(),anyInt(),Mockito.any(Sort.class),(Class<T>) Mockito.anyVararg());
		doReturn(persons).when(operations).findInRange(0,2,null,Person.class);
		doReturn(5L).when(operations).count((Class<T>) Mockito.anyVararg(), anyString());

		Page<T> pagereturn = (Page<T>) simpleAerospikeRepository.findAll(new PageRequest(0, 2));
		Mockito.verify(operations,times(1)).findInRange(0,2,null,Person.class);

		org.junit.Assert.assertEquals(pagereturn,page);
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#exists(java.io.Serializable)}.
	 */
	@Test
	public void testExists() {
		Mockito.when(operations.exists("21",Person.class)).thenReturn(true);
		Boolean exits = simpleAerospikeRepository.exists("21");
		assertThat("Exits is true", exits);
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#findAll()}.
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	@Test
	public void testFindAll() {
		List<Person> persons = new ArrayList<Person>(){{
			add(new Person("one", "Jean", 21));
			add(new Person("two", "Jean2", 22));
			add(new Person("three", "Jean3", 23));
		}};

		Mockito.when((List<Person>)operations.findAll(metadata.getJavaType())).thenReturn(persons);
		List<Person> fetchList = (List<Person>) simpleAerospikeRepository.findAll();
		assertThat(fetchList, containsInAnyOrder(new Person("one", "Jean", 21),new Person("two", "Jean2", 22),new Person("three", "Jean3", 23)));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#findAll(java.lang.Iterable)}.
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	@Test
	public void testFindAllIterableOfID() {
		List<Person> persons = new ArrayList<Person>(){{
			add(new Person("one", "Jean", 21));
			add(new Person("two", "Jean2", 22));
			add(new Person("three", "Jean3", 23));
		}};
		List<String> IDs = new ArrayList<String>(){{
			add("one");
			add("two");
			add("three");
		}};
		SimpleAerospikeRepository<Person, String> aerospikeRepository = (SimpleAerospikeRepository<Person, String>) mock(SimpleAerospikeRepository.class);
		Mockito.when(aerospikeRepository.findAll(IDs)).thenReturn(persons);
		List<Person> fetchList = (List<Person>) aerospikeRepository.findAll(IDs);
		assertThat(fetchList, containsInAnyOrder(new Person("one", "Jean", 21),new Person("two", "Jean2", 22),new Person("three", "Jean3", 23)));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#delete(java.io.Serializable)}.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testDeleteID() {
		SimpleAerospikeRepository<Person, String> aerospikeRepository = (SimpleAerospikeRepository<Person, String>) mock(SimpleAerospikeRepository.class);
		doNothing().when(aerospikeRepository).delete("one");
		aerospikeRepository.delete("one");
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#delete(java.lang.Iterable)}.
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	@Test
	public void testDeleteIterableOfQextendsT() {
		List<Person> persons = new ArrayList<Person>(){{
			add(new Person("one", "Jean", 21));
			add(new Person("two", "Jean2", 22));
			add(new Person("three", "Jean3", 23));
		}};

		SimpleAerospikeRepository<Person, String> aerospikeRepository = (SimpleAerospikeRepository<Person, String>) mock(SimpleAerospikeRepository.class);
		doNothing().when(aerospikeRepository).delete(persons);
		aerospikeRepository.delete(persons);
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#deleteAll()}.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testDeleteAll() {
		SimpleAerospikeRepository<Person, String> aerospikeRepository = (SimpleAerospikeRepository<Person, String>) mock(SimpleAerospikeRepository.class);
		doNothing().when(aerospikeRepository).deleteAll();
		aerospikeRepository.deleteAll();
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository#createIndex(java.lang.Class, java.lang.String, java.lang.String, com.aerospike.client.query.IndexType)}.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testCreateIndex() {
		SimpleAerospikeRepository<Person, String> aerospikeRepository = (SimpleAerospikeRepository<Person, String>) mock(SimpleAerospikeRepository.class);
		doNothing().when(aerospikeRepository).createIndex(Person.class, "index_first_name", "firstName", IndexType.STRING);
		aerospikeRepository.createIndex(Person.class, "index_first_name", "firstName", IndexType.STRING);
	}

}
