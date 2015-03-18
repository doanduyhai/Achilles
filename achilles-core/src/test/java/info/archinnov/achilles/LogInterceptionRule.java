package info.archinnov.achilles;

import static info.archinnov.achilles.logger.AchillesLoggers.ACHILLES_DML_STATEMENT;
import static org.mockito.Mockito.mock;
import java.lang.reflect.Field;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.rules.ExternalResource;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.Log4jLoggerAdapter;

public abstract class LogInterceptionRule extends ExternalResource {

    protected Appender mockedAppender;
    protected Level previousLevel;

    public static DMLStatementInterceptor interceptDMLStatementViaMockedAppender() {
        return new DMLStatementInterceptor();
    }


    public Appender appender() {
        return mockedAppender;
    }

    protected Logger log4jLoggerAccess() {
        try {
            Log4jLoggerAdapter loggerAdapter = (Log4jLoggerAdapter) LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);
            Field loggerField = Log4jLoggerAdapter.class.getDeclaredField("logger");
            loggerField.setAccessible(true);
            return (Logger) loggerField.get(loggerAdapter);
        } catch (NoSuchFieldException | IllegalAccessException cant_access_log4j_logger) {
            throw new RuntimeException("Permission on this JVM doesn't allow to access this field", cant_access_log4j_logger);
        } catch (ClassCastException cant_cast_to_log4j_logger) {
            throw new ClassCastException("It seems the logger implementation is not log4j, please adapt this code");
        }
    }

    public static class DMLStatementInterceptor extends LogInterceptionRule {
        @Override
        protected void before() {
            Logger log4jLogger = log4jLoggerAccess();

            Appender appender = mock(Appender.class);
            log4jLogger.addAppender(appender);
            this.mockedAppender = appender;

            this.previousLevel = log4jLogger.getLevel();
            log4jLogger.setLevel(Level.DEBUG);
        }

        @Override
        protected void after() {
            Logger logger = log4jLoggerAccess();
            logger.removeAppender(mockedAppender);
            logger.setLevel(previousLevel);
        }


    }
}
