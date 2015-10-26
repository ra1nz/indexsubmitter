package net.zsy.indexsubmitter.submitter;

public interface Client {

	void init();

	void close();

	static final String HOSTPORT = "server.hostport";
}
