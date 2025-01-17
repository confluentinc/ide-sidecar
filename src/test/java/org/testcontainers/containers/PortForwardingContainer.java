package org.testcontainers.containers;

import com.github.dockerjava.api.model.ContainerNetwork;
import com.trilead.ssh2.Connection;
import org.testcontainers.utility.DockerImageName;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class PortForwardingContainer {
	public static PortForwardingContainer INSTANCE;

	private final String PASSWORD = UUID.randomUUID().toString();

	private GenericContainer<?> container;

	private final Set<Entry<Integer, Integer>> exposedPorts = Collections.newSetFromMap(new ConcurrentHashMap<>());

	private Connection connection;

	private final Network network;

	public PortForwardingContainer(Network network) {
		this.network = network;

		if (INSTANCE != null) {
			throw new IllegalStateException("PortForwardingContainer already exists");
		} else {
			INSTANCE = this;
		}
	}

	public void start() {
		createSSHSession();
	}

	public void stop() {
		if (connection != null) {
			connection.close();
		}

		if (container != null) {
			container.stop();
		}
	}

	protected void createSSHSession() {
		container = new GenericContainer<>(DockerImageName.parse("testcontainers/sshd:1.2.0"))
				.withExposedPorts(22)
				.withEnv("PASSWORD", PASSWORD)
				.withNetwork(network);
		container.start();

		connection = new Connection(container.getHost(), container.getMappedPort(22));

		try {
			connection.setTCPNoDelay(true);
			connection.connect(
					(hostname, port, serverHostKeyAlgorithm, serverHostKey) -> true,
					(int) Duration.ofSeconds(30).toMillis(),
					(int) Duration.ofSeconds(30).toMillis()
			);

			if (!connection.authenticateWithPassword("root", PASSWORD)) {
				throw new IllegalStateException("Authentication failed.");
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to create SSH connection", e);
		}
	}

	public void exposeHostPort(int port) {
		exposeHostPort(port, port);
	}

	public void exposeHostPort(int hostPort, int containerPort) {
		try {
			if (exposedPorts.add(new AbstractMap.SimpleEntry<>(hostPort, containerPort))) {
				connection.requestRemotePortForwarding(
						"", containerPort, "localhost", hostPort
				);
			}
		} catch (Exception e) {
			throw new IllegalStateException("Failed to expose port " + hostPort, e);
		}
	}

	public Optional<ContainerNetwork> getNetwork() {
		return Optional
				.ofNullable(container)
				.map(GenericContainer::getContainerInfo)
				.flatMap(
						it -> it.getNetworkSettings().getNetworks().values().stream().findFirst()
				);
	}
}