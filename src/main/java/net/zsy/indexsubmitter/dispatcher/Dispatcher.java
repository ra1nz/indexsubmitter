package net.zsy.indexsubmitter.dispatcher;

import net.zsy.indexsubmitter.handler.Handler;

public interface Dispatcher {

	void init();

	void start();

	void stop();

	void add(String message);

	void register(String type, Handler handler);

}
