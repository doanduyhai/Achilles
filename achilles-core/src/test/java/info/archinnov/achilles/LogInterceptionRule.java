package info.archinnov.achilles;

import static info.archinnov.achilles.logger.AchillesLoggers.ACHILLES_DML_STATEMENT;
import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.junit.rules.ExternalResource;
import org.slf4j.LoggerFactory;


public abstract class LogInterceptionRule extends ExternalResource {

    protected Appender<ILoggingEvent> appender;
    protected Level previousLevel;

    public static DMLStatementInterceptor interceptDMLStatementViaMockedAppender() {
        return new DMLStatementInterceptor();
    }


    public Appender appender() {
        return appender;
    }

    protected Logger logBackLogger() {
        return  (Logger)LoggerFactory.getLogger(ACHILLES_DML_STATEMENT);
    }

    public static class DMLStatementInterceptor extends LogInterceptionRule {

        private DMLStatementInterceptor() {
            appender = mock(Appender.class);
        }

        @Override
        protected void before() {
            Logger logger = logBackLogger();
            logger.addAppender(appender);
            this.previousLevel = logger.getLevel();
            logger.setLevel(Level.DEBUG);
        }

        @Override
        protected void after() {
            Logger logger = logBackLogger();
            logger.detachAppender(appender);
            logger.setLevel(previousLevel);
        }


    }
}
