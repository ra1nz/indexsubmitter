package net.zsy.indexsubmitter.submitter;

/**
 * 索引客户端接口
 *
 */
public interface Client {

	void init();

	void close();

	static final String HOSTPORT = "server.hostport";
}
