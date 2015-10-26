package net.zsy.indexsubmitter.handler;

import net.zsy.indexsubmitter.submitter.Submitter;

public abstract class AbstractHandler implements Handler {

	protected Submitter submitter;

	public AbstractHandler(Submitter submitter) {
		this.submitter = submitter;
	}

}
