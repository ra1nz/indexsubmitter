package net.zsy.indexsubmitter.mq;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class MessageQueues {

	private MessageQueues() {
	}

	private static Accepter accepter = null;

	private static Object accepterLock = new Object();

	public static Accepter getAccepter() {
		if (accepter == null) {
			synchronized (accepterLock) {
				if (accepter == null) {
					initAccepter();
				}
			}
		}
		return accepter;
	}

	private static final String PROPERTYFILENAME = "mq.properties";

	private static final String PACKAGE_ACCEPTER = "net.zsy.indexsubmitter.mq.accepter";
	private static final String SUFFIX_ACCEPTER = "Accepter";

	private static void initAccepter() {
		Map<String, String> configurations = loadProperty(PROPERTYFILENAME);
		accepter = getAccepterInstance(configurations.get(MessageQueue.NAME).toString());
		accepter.setConfiguration(configurations);
		accepter.init();
	}

	private static Map<String, String> loadProperty(String propertyName) {
		InputStream in = MessageQueues.class.getResourceAsStream("/" + propertyName);
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

	private static Accepter getAccepterInstance(String name) {
		String className = PACKAGE_ACCEPTER + "." + name + SUFFIX_ACCEPTER;
		try {
			Class<?> c = Class.forName(className);
			return (Accepter) c.newInstance();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("Accepter class not found:" + className);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}
}
