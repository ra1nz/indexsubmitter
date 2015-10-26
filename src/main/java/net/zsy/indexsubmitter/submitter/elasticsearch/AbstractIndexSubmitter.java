package net.zsy.indexsubmitter.submitter.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import net.zsy.indexsubmitter.submitter.IndexClient;
import net.zsy.indexsubmitter.submitter.IndexSubmitter;

public abstract class AbstractIndexSubmitter implements IndexSubmitter, IndexClient {

	public static final String CLUSTERNAME = "es.clustername";
	public static final String INDEXNAME = "es.indexname";
	public static final String INDEXTYPEPREFIX = "es.type";

	private Map<String, String> configurations;

	protected String indexname;

	protected Map<String, String> types;

	protected Client client;

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
							transportClient.addTransportAddress(new InetSocketTransportAddress(
									InetAddress.getByName(hp.split(":")[0]), Integer.valueOf(hp.split(":")[1])));
						} catch (NumberFormatException e) {
							e.printStackTrace();
						} catch (UnknownHostException e) {
							e.printStackTrace();
						}
					}
					indexname = configurations.get(INDEXNAME);
					types = new HashMap<String, String>();
					for (Map.Entry<String, String> entry : configurations.entrySet()) {
						if (entry.getKey().startsWith(INDEXTYPEPREFIX)) {
							String key = entry.getKey();
							key = key.substring(key.indexOf(INDEXTYPEPREFIX) + 1);
							types.put(key, entry.getValue());
						}
					}
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
