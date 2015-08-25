/**
 * 
 */
package org.springframework.data.aerospike.core;

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
@SuiteClasses({ AerospikeTemplateIntegrationTests.class,
		AerospikeTemplateTests.class })
public class AllTests {

}
