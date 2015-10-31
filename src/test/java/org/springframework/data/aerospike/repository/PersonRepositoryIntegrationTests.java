/**
 * 
 */
package org.springframework.data.aerospike.repository;

import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfigPerson.class)
public class PersonRepositoryIntegrationTests extends AbstractPersonRepositoryIntegrationTests {}
