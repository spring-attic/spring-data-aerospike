/**
 * 
 */
package org.springframework.data.aerospike.repository;

import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.test.context.ContextConfiguration;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@ContextConfiguration(classes = TestConfig.class)
public class PersonRepositoryIntegrationTests extends AbstractPersonRepositoryIntegrationTests {}
