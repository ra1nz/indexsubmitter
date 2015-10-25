package net.zsy.indexsubmitter.mq;

import java.util.Map;

public interface MessageQueue {

	void setConfiguration(Map<String, String> configurations);

	void init();

	void start();

	void stop();

	static final String NAME = "mq.name";
	static final String HOSTPORT = "mq.hostport";
	static final String USERNAME = "mq.username";
	static final String PASSWORD = "mq.password";
	static final String QUEUENAME = "mq.queuename";

	static final String CONFIGNAMES = "mq.confignames";

}
