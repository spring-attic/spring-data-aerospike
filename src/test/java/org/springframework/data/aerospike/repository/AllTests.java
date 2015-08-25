/**
 * 
 */
package org.springframework.data.aerospike.repository;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@RunWith(Suite.class)
@SuiteClasses({ ContactRepositoryIntegrationTest.class,
		CustomerRepositoriesIntegrationTests.class,
		PersonRepositoryIntegrationTests.class })
public class AllTests {

}
