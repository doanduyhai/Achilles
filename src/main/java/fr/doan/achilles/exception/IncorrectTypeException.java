package fr.doan.achilles.exception;

/**
 * IncorrectTypeException
 * 
 * @author DuyHai DOAN
 * 
 */
public class IncorrectTypeException extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	public IncorrectTypeException() {
		super();
	}

	public IncorrectTypeException(String message) {
		super(message);
	}

	public IncorrectTypeException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
