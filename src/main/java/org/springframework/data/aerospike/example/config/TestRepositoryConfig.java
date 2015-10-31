/**
 * 
 */
package org.springframework.data.aerospike.example.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
@EnableAerospikeRepositories(basePackages = "org.springframework.data.aerospike.example")
public class TestRepositoryConfig {
	public @Bean(destroyMethod = "close") AerospikeClient aerospikeClient() {

		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = true;
		policy.timeout = 2000;

		return new AerospikeClient(policy, "127.0.0.1", 3000);
	}

	public @Bean AerospikeTemplate aerospikeTemplate() {
		return new AerospikeTemplate(aerospikeClient(), "test");
	}

}
