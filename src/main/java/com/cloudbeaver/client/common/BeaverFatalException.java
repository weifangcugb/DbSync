package com.cloudbeaver.client.common;

public class BeaverFatalException extends Exception {

	public BeaverFatalException(String msg, Throwable e) {
		super(msg, e);
	}

	public BeaverFatalException(String msg) {
		super(msg);
	}

}
