package fr.doan.achilles.exception;

/**
 * BeanMappingException
 * 
 * @author DuyHai DOAN
 * 
 */
public class BeanMappingException extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	public BeanMappingException() {
		super();
	}

	public BeanMappingException(String message) {
		super(message);
	}

	public BeanMappingException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
