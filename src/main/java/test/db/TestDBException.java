package test.db;

public class TestDBException extends RuntimeException {

	private static final long serialVersionUID = 2264700288886772589L;

	public TestDBException() {
	}

	public TestDBException(String message) {
		super(message);
	}

	public TestDBException(String message, Throwable cause) {
		super(message, cause);
	}

	public TestDBException(Throwable cause) {
		super(cause);
	}

	public TestDBException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
