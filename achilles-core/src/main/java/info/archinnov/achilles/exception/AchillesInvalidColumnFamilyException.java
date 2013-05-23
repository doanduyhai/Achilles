package info.archinnov.achilles.exception;

/**
 * AchillesInvalidColumnFamilyException
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesInvalidColumnFamilyException extends RuntimeException
{

	private static final long serialVersionUID = 1L;

	public AchillesInvalidColumnFamilyException() {
		super();
	}

	public AchillesInvalidColumnFamilyException(String message) {
		super(message);
	}

	public AchillesInvalidColumnFamilyException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
