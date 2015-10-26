package net.zsy.indexsubmitter.submitter;

public interface IndexClient {

	void init();

	void close();

	static final String HOSTPORT = "server.hostport";
}
