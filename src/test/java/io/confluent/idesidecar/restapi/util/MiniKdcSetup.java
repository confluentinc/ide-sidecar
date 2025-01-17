package io.confluent.idesidecar.restapi.util;

import org.apache.hadoop.minikdc.MiniKdc;

import java.io.File;
import java.nio.file.Files;
import java.util.Properties;

public class MiniKdcSetup {

	private MiniKdc kdc;
	private File workDir;
	private File keytabFile;

	public void startKdc() {
		try {
			workDir = Files.createTempDirectory("miniKdc").toFile();
			keytabFile = new File(workDir, "kafka.keytab");

			Properties kdcConf = MiniKdc.createConf();
			kdcConf.setProperty(MiniKdc.DEBUG, "true");
			kdc = new MiniKdc(kdcConf, workDir);
			kdc.start();
			kdc.createPrincipal(keytabFile, "kafka/broker@EXAMPLE.COM", "kafka/client@EXAMPLE.COM");
		} catch (Exception e) {
			throw new RuntimeException("Failed to start MiniKdc", e);
		}
	}

	public void stopKdc() {
		if (kdc != null) {
			kdc.stop();
		}
	}

	public File getKeytabFile() {
		return keytabFile;
	}

	public int getPort() {
		return kdc.getPort();
	}

	public File getKrb5conf() {
		return kdc.getKrb5conf();
	}

	public File getWorkDir() {
		return workDir;
	}
}