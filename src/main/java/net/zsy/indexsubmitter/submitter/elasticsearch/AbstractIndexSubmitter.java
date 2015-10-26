package net.zsy.indexsubmitter.submitter.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import net.zsy.indexsubmitter.submitter.Client;
import net.zsy.indexsubmitter.submitter.Submitter;

public abstract class AbstractIndexSubmitter implements Submitter, Client {

	public static final String CLUSTERNAME = "es.clustername";
	public static final String INDEXNAME = "es.indexname";

	private Map<String, String> configurations;

	protected String indexname;
	
	protected org.elasticsearch.client.Client client;

	private Boolean inited = false;

	public AbstractIndexSubmitter(Map<String, String> configurations) {
		this.configurations = configurations;
	}

	@Override
	public void init() {
		if (!inited) {
			synchronized (inited) {
				if (!inited) {
					Settings settings = Settings.settingsBuilder().put("cluster.name", configurations.get(CLUSTERNAME))
							.build();
					TransportClient transportClient = TransportClient.builder().settings(settings).build();

					String hostports = configurations.get(HOSTPORT);

					if (StringUtils.isBlank(hostports)) {
						throw new RuntimeException("hostport not configured.");
					}
					String[] hps = hostports.split(",");
					for (String hp : hps) {
						try {
							String[] arrhp = hp.split(":");
							transportClient.addTransportAddress(new InetSocketTransportAddress(
									InetAddress.getByName(arrhp[0]), Integer.valueOf(arrhp[1])));
						} catch (NumberFormatException e) {
							e.printStackTrace();
						} catch (UnknownHostException e) {
							e.printStackTrace();
						}
					}
					indexname = configurations.get(INDEXNAME);
					
					client = transportClient;
					inited = true;
				}
			}
		}
	}

	@Override
	public void close() {
		if (inited && client != null) {
			client.close();
		}
	}

}
