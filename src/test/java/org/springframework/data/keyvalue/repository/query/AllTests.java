/**
 * 
 */
package org.springframework.data.keyvalue.repository.query;

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
@SuiteClasses({ AerospikeQueryCreatorUnitTests.class,
		SpelQueryCreatorUnitTests.class })
public class AllTests {

}
