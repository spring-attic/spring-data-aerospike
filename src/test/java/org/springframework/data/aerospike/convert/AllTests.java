/**
 * 
 */
package org.springframework.data.aerospike.convert;

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
		AerospikeReadDataTest.class,
		MappingAerospikeConverterTest.class,
		MappingAerospikeConverterDeprecatedTest.class })
public class AllTests {

}
