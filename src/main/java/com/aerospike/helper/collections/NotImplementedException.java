package com.aerospike.helper.collections;

public class NotImplementedException extends RuntimeException {

	/**
	 * This exception indicates the the method is not implemented.
	 */
	private static final long serialVersionUID = 6989807145209111662L;

	public NotImplementedException() {
		super("Method not implemented");
	}

	public NotImplementedException(String message, Throwable cause, boolean enableSuppression,
								   boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public NotImplementedException(String message, Throwable cause) {
		super(message, cause);
	}

	public NotImplementedException(String message) {
		super(message);
	}

	public NotImplementedException(Throwable cause) {
		super(cause);
	}

}
