package info.archinnov.achilles.exception;

/**
 * AchillesException
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    public AchillesException(Throwable throwable) {
        super(throwable);
    }

    public AchillesException() {
        super();
    }

    public AchillesException(String message) {
        super(message);
    }

    public AchillesException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
