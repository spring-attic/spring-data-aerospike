/**
 * 
 */
package org.springframework.data.aerospike.mapping;

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
@SuiteClasses({ AerospikeMappingContextTest.class,
		AerospikeMetadataBinTest.class,
		CachingAerospikePersistentPropertyTest.class })
public class AllTests {

}
