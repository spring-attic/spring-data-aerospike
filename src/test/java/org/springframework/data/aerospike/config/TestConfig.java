/**
 * 
 */
package org.springframework.data.aerospike.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.EmbeddedAerospikeInfo;
import org.springframework.data.aerospike.cache.AerospikeCacheManager;
import org.springframework.data.aerospike.cache.AerospikeCacheMangerTests.CachingComponent;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.repository.ContactRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

import com.aerospike.client.AerospikeClient;
import org.springframework.data.aerospike.sample.CustomerRepository;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@Configuration
@EnableAerospikeRepositories(basePackageClasses = {ContactRepository.class, CustomerRepository.class})
@EnableCaching
@EnableAutoConfiguration
public class TestConfig extends CachingConfigurerSupport {

	public @Bean AerospikeTemplate aerospikeTemplate(AerospikeClient aerospikeClient, EmbeddedAerospikeInfo info) {
		return new AerospikeTemplate(aerospikeClient, info.getNamespace());
	}

	public @Bean AerospikeCacheManager cacheManager(AerospikeClient aerospikeClient) {
		return new AerospikeCacheManager(aerospikeClient);
	}

	public @Bean CachingComponent cachingComponent() {
		return new CachingComponent();
	}
}
