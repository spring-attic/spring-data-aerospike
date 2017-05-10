package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.StartupCheckStrategy;

@Configuration
public class EmbeddedAerospikeAutoConfiguration {

    @Bean(initMethod = "start", destroyMethod = "stop")
    public GenericContainer aerosike() {
        GenericContainer aerosike =
                new GenericContainer("aerospike:3.13.0.8")
                        .withStartupCheckStrategy(new EmbeddedAerospikeStartupCheckStrategy())
                        .withExposedPorts(TestConstants.AS_PORT)
                        .withClasspathResourceMapping("aerospike.conf", "/etc/aerospike/aerospike.conf", BindMode.READ_ONLY);
        return aerosike;
    }

    @Bean
    public EmbeddedAerospikeInfo embeddedAerospikeInfo(GenericContainer aerosike) {
        Integer mappedPort = aerosike.getMappedPort(TestConstants.AS_PORT);
        String host = aerosike.getContainerIpAddress();
        return new EmbeddedAerospikeInfo(host, mappedPort, TestConstants.AS_NAMESPACE);
    }

    @Bean(destroyMethod = "close")
    public AerospikeClient aerospikeClient(EmbeddedAerospikeInfo info) {

        ClientPolicy policy = new ClientPolicy();
        policy.failIfNotConnected = true;
        policy.timeout = TestConstants.AS_TIMEOUT;

        AerospikeClient client = new AerospikeClient(policy, info.getHost(), info.getPort());
        client.writePolicyDefault.expiration = -1;
        return client;
    }

}
