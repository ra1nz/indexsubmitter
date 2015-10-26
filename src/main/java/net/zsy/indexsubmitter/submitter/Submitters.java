package net.zsy.indexsubmitter.submitter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import net.zsy.indexsubmitter.submitter.elasticsearch.AbandonOutOfDateSubmitter;
import net.zsy.indexsubmitter.submitter.elasticsearch.GenericIndexSubmitter;

public abstract class Submitters {

	private Submitters() {
	}

	private static final ConcurrentHashMap<String, Submitter> submitters = new ConcurrentHashMap<String, Submitter>();

	private static final String SUBMITTER_GENERIC = "generic";
	private static final String SUBMITTER_AOOD = "abandonoutofdate";

	private static final String PROPERTYFILENAME = "indexserver.properties";

	public static Submitter getGenericSubmitter() {
		if (submitters.containsKey(SUBMITTER_GENERIC)) {
			return submitters.get(SUBMITTER_GENERIC);
		}
		return initGenericSubmitter();
	}

	public static ConcurrentSubmitter getAOODSubmitter(int submitThreadNum) {
		if (submitters.containsKey(SUBMITTER_AOOD)) {
			return (ConcurrentSubmitter) submitters.get(SUBMITTER_AOOD);
		}
		return (ConcurrentSubmitter) initAOODSubmitter(submitThreadNum);
	}

	private static Submitter initGenericSubmitter() {
		Map<String, String> configurations = loadProperty(PROPERTYFILENAME);
		Submitter submitter = new GenericIndexSubmitter(configurations);
		submitters.putIfAbsent(SUBMITTER_GENERIC, submitter);
		return submitter;
	}

	private static Submitter initAOODSubmitter(int submitThreadNum) {
		Map<String, String> configurations = loadProperty(PROPERTYFILENAME);
		Submitter submitter = new AbandonOutOfDateSubmitter(configurations, submitThreadNum);
		submitters.putIfAbsent(SUBMITTER_AOOD, submitter);
		return submitter;
	}

	private static Map<String, String> loadProperty(String propertyName) {
		InputStream in = Submitters.class.getResourceAsStream("/" + propertyName);
		Properties properties = new Properties();
		Map<String, String> configurations = new HashMap<String, String>();
		try {
			properties.load(in);
			Enumeration<Object> keys = properties.keys();

			while (keys.hasMoreElements()) {
				Object object = (Object) keys.nextElement();
				configurations.put(object.toString(), properties.getProperty(object.toString()));
			}
			System.out.println(propertyName + "properties loaded:");
			System.out.println(configurations);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return configurations;
	}
}
