package fr.doan.achilles.exception;

/**
 * ValidationException
 * 
 * @author DuyHai DOAN
 * 
 */
public class ValidationException extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	public ValidationException() {
		super();
	}

	public ValidationException(String message) {
		super(message);
	}

	public ValidationException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
