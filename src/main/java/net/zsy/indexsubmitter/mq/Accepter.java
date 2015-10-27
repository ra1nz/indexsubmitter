package net.zsy.indexsubmitter.mq;

/**
 * 消息队列接收器接口
 *
 */
public interface Accepter extends MessageQueue {
	
	String accept();
	
}
