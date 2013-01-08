package fr.doan.achilles.exception;

/**
 * InvalidBeanException
 * 
 * @author DuyHai DOAN
 * 
 */
public class InvalidBeanException extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	public InvalidBeanException() {
		super();
	}

	public InvalidBeanException(String message) {
		super(message);
	}

	public InvalidBeanException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
