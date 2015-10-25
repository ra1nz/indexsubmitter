package net.zsy.indexsubmitter.mq;

public interface Accepter extends MessageQueue {
	
	String accept();
	
}
