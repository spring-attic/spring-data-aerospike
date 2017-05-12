/**
 *
 */
package org.springframework.data.keyvalue.repository.query;

import static org.springframework.data.aerospike.repository.query.StubParameterAccessor.getAccessor;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.core.Person;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.repository.BaseRepositoriesIntegrationTests;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeQueryCreatorUnitTests extends BaseRepositoriesIntegrationTests {

	MappingContext<?, AerospikePersistentProperty> context;
	Method findByFirstname, findByFirstnameAndFriend, findByFirstnameNotNull;
	@Mock
	AerospikeConverter converter;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
		context = new AerospikeMappingContext();
	}

	@SuppressWarnings({ "unused", "rawtypes" })
	@Test
	public void createsQueryCorrectly() throws Exception {
		PartTree tree = new PartTree("findByFirstName", Person.class);

//		AerospikeQueryCreator creator = new AerospikeQueryCreator(tree, getAccessor(converter, "Oliver"), context);
//		Query query = creator.createQuery();
		//assertThat(query, is(Query.query(Criteria.where("firstName").is("Oliver"))));
	}

}
