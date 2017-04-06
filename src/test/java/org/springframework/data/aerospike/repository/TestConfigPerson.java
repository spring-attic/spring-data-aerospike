/**
 *
 */
package org.springframework.data.aerospike.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.EmbeddedAerospikeInfo;
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
@EnableAutoConfiguration
public class TestConfigPerson {

	public @Bean AerospikeTemplate aerospikeTemplate(AerospikeClient aerospikeClient, EmbeddedAerospikeInfo info) {
		return new AerospikeTemplate(aerospikeClient, info.getNamespace());
	}
}
