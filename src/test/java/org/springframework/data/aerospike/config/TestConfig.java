/**
 *
 */
package org.springframework.data.aerospike.config;

import com.aerospike.client.Host;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.EmbeddedAerospikeInfo;
import org.springframework.data.aerospike.cache.AerospikeCacheManager;
import org.springframework.data.aerospike.cache.AerospikeCacheMangerTests.CachingComponent;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.repository.ContactRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

import com.aerospike.client.AerospikeClient;
import org.springframework.data.aerospike.sample.CustomerRepository;

import java.util.Collection;
import java.util.Collections;
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
public class TestConfig extends AbstractAerospikeDataConfiguration  {

	@Autowired
	private EmbeddedAerospikeInfo info;

	public @Bean CacheManager cacheManager(AerospikeClient aerospikeClient, MappingAerospikeConverter aerospikeConverter) {
		return new AerospikeCacheManager(aerospikeClient, aerospikeConverter);
	}

	public @Bean CachingComponent cachingComponent() {
		return new CachingComponent();
	}

	@Override
	protected Collection<Host> getHosts() {
		return Collections.singleton(new Host(info.getHost(), info.getPort()));
	}

	@Override
	protected String nameSpace() {
		return info.getNamespace();
	}
}
