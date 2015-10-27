package net.zsy.indexsubmitter.submitter;

/**
 * 提交器接口定义
 *
 */
public interface Submitter {

	void submit(String id, String type, Long timestamp, String json);

	static final String TIMESTAMP = "timestamp";
	static final String DATA = "data";
	static final String TYPE = "type";
	static final String ID = "id";
}
