package info.archinnov.achilles.exception;

/**
 * InvalidColumnFamilyException
 * 
 * @author DuyHai DOAN
 * 
 */
public class InvalidColumnFamilyException extends RuntimeException
{

	private static final long serialVersionUID = 1L;

	public InvalidColumnFamilyException() {
		super();
	}

	public InvalidColumnFamilyException(String message) {
		super(message);
	}

	public InvalidColumnFamilyException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
