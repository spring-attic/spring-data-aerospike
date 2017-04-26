/**
 *
 */
package org.springframework.data.aerospike.example.config;

import com.aerospike.client.Host;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

import java.util.Collection;
import java.util.Collections;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
@Configuration
@EnableAerospikeRepositories(basePackages = "org.springframework.data.aerospike.example")
public class TestRepositoryConfig extends AbstractAerospikeDataConfiguration {

    @Override
    protected Collection<Host> getHosts() {
        return Collections.singleton(new Host("192.168.105.146", 3000));
    }

    @Override
    protected String nameSpace() {
        return "test";
    }

}
