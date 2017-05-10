package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.startupcheck.StartupCheckStrategy;
import org.testcontainers.shaded.com.github.dockerjava.api.DockerClient;
import org.testcontainers.shaded.com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.shaded.com.github.dockerjava.api.model.ExposedPort;
import org.testcontainers.shaded.com.github.dockerjava.api.model.NetworkSettings;
import org.testcontainers.shaded.com.github.dockerjava.api.model.Ports;
import org.testcontainers.utility.DockerStatus;

public class EmbeddedAerospikeStartupCheckStrategy extends StartupCheckStrategy {

	private final int aerospikePort;

	public EmbeddedAerospikeStartupCheckStrategy(int aerospikePort) {
		this.aerospikePort = aerospikePort;
	}

	@Override
	public StartupStatus checkStartupState(DockerClient dockerClient, String containerId) {
		InspectContainerResponse response = dockerClient.inspectContainerCmd(containerId).exec();

		InspectContainerResponse.ContainerState state = response.getState();
		if (!DockerStatus.isContainerExitCodeSuccess(state)) {
			return StartupStatus.FAILED;
		}

		if (!state.getRunning()) {
			return StartupStatus.NOT_YET_KNOWN;
		}

		return checkStartupStateByConnectingToAerospike(response.getNetworkSettings());
	}

	private StartupStatus checkStartupStateByConnectingToAerospike(NetworkSettings networkSettings) {
		int port = getMappedPort(networkSettings, aerospikePort);
		String host = DockerClientFactory.instance().dockerHostIpAddress();

		try (AerospikeClient client = new AerospikeClient(host, port)) {
			return StartupStatus.SUCCESSFUL;
		} catch (AerospikeException.Connection ignore) {
			return StartupStatus.NOT_YET_KNOWN;
		}
	}

	private int getMappedPort(NetworkSettings networkSettings, int originalPort) {
		ExposedPort exposedPort = new ExposedPort(originalPort);
		Ports.Binding[] binding = networkSettings.getPorts().getBindings().get(exposedPort);

		return Integer.valueOf(binding[0].getHostPortSpec());
	}
}
