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
@SuiteClasses({ AerospikeDataTest.class,
		MappingAerospikeConverterConversionTest.class,
		MappingAerospikeConverterTest.class })
public class AllTests {

}
