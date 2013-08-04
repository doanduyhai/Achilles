package info.archinnov.achilles.test.integration.utils;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;

/**
 * HectorLogAsserter
 * 
 * @author DuyHai DOAN
 * 
 */
public class CassandraLogAsserter
{
    private static final String STORAGE_PROXY_LOGGER = "org.apache.cassandra.service.StorageProxy";
    private Logger storageProxyLogger = Logger.getLogger(STORAGE_PROXY_LOGGER);
    private WriterAppender writerAppender;
    private ByteArrayOutputStream logStream;

    public void prepareLogLevel()
    {
        logStream = new ByteArrayOutputStream();
        storageProxyLogger.setLevel(Level.TRACE);
        writerAppender = new WriterAppender();
        writerAppender.setWriter(new OutputStreamWriter(logStream));
        writerAppender.setLayout(new PatternLayout("%-5p [%d{ABSOLUTE}][%x] %c@:%M %m %n"));
        storageProxyLogger.addAppender(writerAppender);

    }

    public void assertConsistencyLevels(ConsistencyLevel read, ConsistencyLevel write)
    {
        String[] standardOutputs = StringUtils.split(logStream.toString(), "\n");
        try
        {
            for (String logLine : standardOutputs)
            {
                if (logLine.contains("fetchRows Command/ConsistencyLevel is"))
                {
                    assertThat(logLine).contains("/" + read.name());
                }

                if (logLine.contains("mutate Mutations/ConsistencyLevel are [RowMutation"))
                {
                    assertThat(logLine).contains("/" + write.name());
                }
            }
        } finally
        {
            logStream = null;
            storageProxyLogger.setLevel(Level.WARN);
            storageProxyLogger.removeAppender(writerAppender);
        }
    }
}
