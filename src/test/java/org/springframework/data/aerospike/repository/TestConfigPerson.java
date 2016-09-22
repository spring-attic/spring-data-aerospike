/**
 * 
 */
package org.springframework.data.aerospike.repository;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.TestConstants;
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

		AerospikeClient client = new AerospikeClient(policy, TestConstants.AS_CLUSTER, TestConstants.AS_PORT); //AWS us-east
		client.writePolicyDefault.expiration = -1;
		return client;
	}

	public @Bean AerospikeTemplate aerospikeTemplate() {
		return new AerospikeTemplate(aerospikeClient(), "test"); 
	}
}
