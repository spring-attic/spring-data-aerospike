/**
 * 
 */
package org.springframework.data.aerospike.config;

import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.TestConstants;
import org.springframework.data.aerospike.cache.AerospikeCacheManager;
import org.springframework.data.aerospike.cache.AerospikeCacheMangerTests.CachingComponent;
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
@EnableCaching
public class TestConfig extends CachingConfigurerSupport {

	public @Bean(destroyMethod = "close") AerospikeClient aerospikeClient() {

		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = true;
		policy.timeout = TestConstants.AS_TIMEOUT;

		AerospikeClient client = new AerospikeClient(policy, TestConstants.AS_CLUSTER, TestConstants.AS_PORT); //AWS us-east
		client.writePolicyDefault.expiration = -1;
		return client;
	}

	public @Bean AerospikeTemplate aerospikeTemplate() {
		return new AerospikeTemplate(aerospikeClient(), TestConstants.AS_NAMESPACE); // TODO verify correct place for namespace
	}

	public @Bean AerospikeCacheManager cacheManager() {
		return new AerospikeCacheManager(aerospikeClient());
	}

	public @Bean CachingComponent cachingComponent() {
		return new CachingComponent();
	}
}
