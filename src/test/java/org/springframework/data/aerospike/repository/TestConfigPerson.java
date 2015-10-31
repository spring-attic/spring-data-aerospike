/**
 * 
 */
package org.springframework.data.aerospike.repository;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.Constants;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;

/**
*
*
* @author Peter Milne
* @author Jean Mercier
*
*/
@Configuration
@EnableAerospikeRepositories(basePackageClasses = ContactRepository.class)
public class TestConfigPerson {
	public @Bean(destroyMethod = "close") AerospikeClient aerospikeClient() {

		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = true;
		policy.timeout = 2000;

		return new AerospikeClient(policy, Constants.AS_CLUSTER, Constants.AS_PORT);
	}

	public @Bean AerospikeTemplate aerospikeTemplate() {
		return new AerospikeTemplate(aerospikeClient(), "test"); 
	}
}
