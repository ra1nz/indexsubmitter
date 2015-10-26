package net.zsy.indexsubmitter.submitter;

public interface IndexSubmitter {
	void submit(String message);

	static final String TIMESTAMP = "timestamp";
	static final String DATA = "data";
	static final String TYPE = "type";
	static final String ID = "id";
}
