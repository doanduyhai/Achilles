package info.archinnov.achilles.exception;

/**
 * AchillesBeanMappingException
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesBeanMappingException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    public AchillesBeanMappingException() {
        super();
    }

    public AchillesBeanMappingException(String message) {
        super(message);
    }

    public AchillesBeanMappingException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
