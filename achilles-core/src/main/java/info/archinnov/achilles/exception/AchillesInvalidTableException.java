package info.archinnov.achilles.exception;

/**
 * AchillesInvalidTableException
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesInvalidTableException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    public AchillesInvalidTableException() {
        super();
    }

    public AchillesInvalidTableException(String message) {
        super(message);
    }

    public AchillesInvalidTableException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
