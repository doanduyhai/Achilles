/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.test.integration.utils;

import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.split;
import static org.fest.assertions.api.Assertions.assertThat;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import info.archinnov.achilles.type.ConsistencyLevel;
import org.junit.ComparisonFailure;

public class CassandraLogAsserter {
    private static final String DRIVER_CONNECTION_LOGGER = "com.datastax.driver.core.Connection";
    private Logger storageProxyLogger = Logger.getLogger(DRIVER_CONNECTION_LOGGER);
    private WriterAppender writerAppender;
    protected ByteArrayOutputStream logStream;
    private Pattern pattern = Pattern.compile("writing request [A-Z]+");

    public void prepareLogLevel() {
        logStream = new ByteArrayOutputStream();
        writerAppender = new WriterAppender();
        writerAppender.setWriter(new OutputStreamWriter(logStream));
        writerAppender.setLayout(new PatternLayout("%m %n"));
        storageProxyLogger.removeAllAppenders();
        storageProxyLogger.addAppender(writerAppender);
        storageProxyLogger.setLevel(Level.TRACE);

    }

    public void assertConsistencyLevels(ConsistencyLevel... consistencyLevels) {
        final List<ConsistencyLevel> expectedConsistencyLevels = asList(consistencyLevels);
        final Iterator<ConsistencyLevel> clIterator = expectedConsistencyLevels.iterator();
        final Iterator<String> standardOutputs = asList(split(logStream.toString(), "\n")).iterator();

        List<ConsistencyLevel> founds = new LinkedList<>();
        ConsistencyLevel consistencyLevel = clIterator.next();

        try {
            while (standardOutputs.hasNext()) {
                final String logLine = standardOutputs.next();

                if (pattern.matcher(logLine).find() && checkForConsistency(consistencyLevel, logLine)) {
                    founds.add(consistencyLevel);
                    if (clIterator.hasNext()) {
                        consistencyLevel = clIterator.next();
                    } else {
                        break;
                    }
                }
            }

            assertThat(founds).describedAs("expected consistency levels").isEqualTo(expectedConsistencyLevels);

        } finally {
            logStream = null;
            storageProxyLogger.setLevel(Level.WARN);
            storageProxyLogger.removeAppender(writerAppender);
        }
    }

    protected boolean checkForConsistency(ConsistencyLevel consistencyLevel, String logLine) {
        return logLine.contains("cl=" + consistencyLevel.name()) || logLine.contains("at consistency "+consistencyLevel.name());
    }

    public void assertSerialConsistencyLevels(ConsistencyLevel serialConsistencyLevel, ConsistencyLevel... consistencyLevels) {
        final List<ConsistencyLevel> expectedConsistencyLevels = consistencyLevels != null ? asList(consistencyLevels) : Arrays.<ConsistencyLevel>asList();
        final Iterator<ConsistencyLevel> clIterator = expectedConsistencyLevels.iterator();
        final Iterator<String> standardOutputs = asList(split(logStream.toString(), "\n")).iterator();

        try {

            if (expectedConsistencyLevels.isEmpty()) {
                boolean foundSerialCL = false;
                while (standardOutputs.hasNext()) {
                    final String logLine = standardOutputs.next();

                    if (pattern.matcher(logLine).find() && logLine.contains("serialCl=" + serialConsistencyLevel.name())) {
                        foundSerialCL |= true;
                    }
                }

                if (!foundSerialCL) {
                    throw new ComparisonFailure("Cannot find serialConsistencyLevel", serialConsistencyLevel.name(), "nothing found");
                }
            } else {

                List<ConsistencyLevel> founds = new LinkedList<>();
                ConsistencyLevel consistencyLevel = clIterator.next();
                boolean foundSerialCL = false;

                while (standardOutputs.hasNext()) {
                    final String logLine = standardOutputs.next();

                    if (pattern.matcher(logLine).find() && logLine.contains("cl=" + consistencyLevel.name())) {
                        founds.add(consistencyLevel);

                        if (logLine.contains("serialCl=" + serialConsistencyLevel.name())) {
                            foundSerialCL |= true;
                        }

                        if (clIterator.hasNext()) {
                            consistencyLevel = clIterator.next();
                        } else {
                            break;
                        }

                    }
                }

                assertThat(foundSerialCL).describedAs("serialConsistencyLevel " + serialConsistencyLevel.name() + " has been found").isTrue();
                assertThat(founds).describedAs("expected consistency levels").isEqualTo(expectedConsistencyLevels);
            }


        } finally {
            logStream = null;
            storageProxyLogger.setLevel(Level.WARN);
            storageProxyLogger.removeAppender(writerAppender);
        }
    }
}
