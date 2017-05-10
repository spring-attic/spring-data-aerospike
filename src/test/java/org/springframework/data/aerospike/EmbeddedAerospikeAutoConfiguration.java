package org.springframework.data.aerospike;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

@Configuration
public class EmbeddedAerospikeAutoConfiguration {

	private static final int PORT = 3000;
	private static final String NAMESPACE = "test";

	@Bean(initMethod = "start", destroyMethod = "stop")
	public GenericContainer aerosike() {
		GenericContainer aerosike =
				new GenericContainer("aerospike:3.13.0.8")
						.withStartupCheckStrategy(new EmbeddedAerospikeStartupCheckStrategy(PORT))
						.withExposedPorts(PORT)
						.withClasspathResourceMapping("aerospike.conf", "/etc/aerospike/aerospike.conf", BindMode.READ_ONLY);
		return aerosike;
	}

	@Bean
	public EmbeddedAerospikeInfo embeddedAerospikeInfo(GenericContainer aerosike) {
		Integer mappedPort = aerosike.getMappedPort(PORT);
		String host = aerosike.getContainerIpAddress();
		return new EmbeddedAerospikeInfo(host, mappedPort, NAMESPACE);
	}

}
