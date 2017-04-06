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
@SuiteClasses({
		CustomerRepositoriesIntegrationTests.class,
		PersonRepositoryIntegrationTests.class,
		RepositoriesIntegrationTests.class
})
public class AllTests {

}
