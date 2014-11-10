package info.archinnov.achilles.configuration;

import static org.slf4j.LoggerFactory.getLogger;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;

public class DefaultExecutorThreadFactory implements ThreadFactory {

    private static final Logger logger = getLogger("achilles-default-executor");

    private final AtomicInteger threadNumber = new AtomicInteger(0);
    private Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("", e);
        }
    };

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName("achilles-default-executor-" + threadNumber.incrementAndGet());
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return thread;
    }
}
