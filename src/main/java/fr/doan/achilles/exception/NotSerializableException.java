package fr.doan.achilles.exception;

public class NotSerializableException extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	public NotSerializableException() {
		super();
	}

	public NotSerializableException(String message) {
		super(message);
	}

	public NotSerializableException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
