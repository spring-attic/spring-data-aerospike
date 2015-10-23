/**
 * 
 */
package org.springframework.data.aerospike.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.repository.ContactRepository;
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
public class TestConfig {
	public @Bean(destroyMethod = "close") AerospikeClient aerospikeClient() {

		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = true;
		policy.timeout = 2000;

<<<<<<< HEAD
		return new AerospikeClient(policy, "52.23.205.208", 3000);
=======
		return new AerospikeClient(policy, "52.23.205.208", 3000); //AWS us-east
>>>>>>> f5d11a27ce0f7dc9dbf0ca5446224e8ba471a1e5
	}

	public @Bean AerospikeTemplate aerospikeTemplate() {
		return new AerospikeTemplate(aerospikeClient(), "bar"); // TODO verify correct place for namespace
	}

}
