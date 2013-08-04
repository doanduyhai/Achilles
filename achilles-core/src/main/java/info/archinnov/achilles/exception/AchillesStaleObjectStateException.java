package info.archinnov.achilles.exception;

/**
 * AchillesStaleObjectStateException
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesStaleObjectStateException extends Exception
{
    private static final long serialVersionUID = 1L;

    public AchillesStaleObjectStateException() {
        super();
    }

    public AchillesStaleObjectStateException(String message) {
        super(message);
    }

    public AchillesStaleObjectStateException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
