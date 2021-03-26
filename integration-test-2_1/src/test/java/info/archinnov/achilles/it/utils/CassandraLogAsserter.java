/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.it.utils;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.split;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.ComparisonFailure;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Joiner;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.OutputStreamAppender;


public class CassandraLogAsserter {
    private static final LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

    private static final String DRIVER_CONNECTION_LOGGER = "com.datastax.driver.core.Connection";
    private static final String DEFAULT_PATTERN = "%msg%n";
    protected ByteArrayOutputStream logStream;
    private Logger logger = loggerContext.getLogger(DRIVER_CONNECTION_LOGGER);
    private OutputStreamAppender<ILoggingEvent> writerAppender;
    private Pattern pattern = Pattern.compile("writing request [A-Z]+");

    public void prepareLogLevelForDriverConnection() {
        prepareLogLevel(DRIVER_CONNECTION_LOGGER);
    }

    public void prepareLogLevel(String loggerName, String logMessagePattern) {
        logger = (Logger) LoggerFactory.getLogger(loggerName);

        logger.detachAndStopAllAppenders();
        final PatternLayoutEncoder patternLayout = new PatternLayoutEncoder();
        patternLayout.setContext(loggerContext);
        patternLayout.setPattern(logMessagePattern);
        patternLayout.start();

        writerAppender = new OutputStreamAppender<>();
        logStream = new ByteArrayOutputStream();
        writerAppender.setEncoder(patternLayout);
        writerAppender.setOutputStream(logStream);
        writerAppender.setContext(loggerContext);
        writerAppender.setName(this.getClass().getSimpleName());
        writerAppender.start();

        logger.addAppender(writerAppender);
        logger.setAdditive(false);
        logger.setLevel(Level.TRACE);
    }

    public void prepareLogLevel(String loggerName) {
        prepareLogLevel(loggerName, DEFAULT_PATTERN);
    }

    public void assertContains(String text) {
        assertPatternToBe(text, true);
    }

    public void assertNotContains(String text) {
        assertPatternToBe(text, false);
    }

    private void assertPatternToBe(String text, boolean present) {
        final Iterator<String> standardOutputs = asList(split(logStream.toString(), "\n")).iterator();
        try {
            boolean textFound = false;
            while (standardOutputs.hasNext()) {
                final String logLine = standardOutputs.next();
                if (logLine.contains(text)) {
                    textFound = true;
                    break;
                }
            }
            assertThat(textFound).describedAs("Expected '" + text + "' to be found in the logs").isEqualTo(present);

        } finally {
            logStream = null;
            logger.setLevel(Level.WARN);
            logger.detachAppender(writerAppender);
            writerAppender.stop();
        }
    }

    public void assertConsistencyLevels(ConsistencyLevel... consistencyLevels) throws IOException {
        final List<ConsistencyLevel> expectedConsistencyLevels = asList(consistencyLevels);
        final Iterator<ConsistencyLevel> clIterator = expectedConsistencyLevels.iterator();

        final Iterator<String> standardOutputs = asList(split(logStream.toString(), "\n")).iterator();

        List<ConsistencyLevel> founds = new LinkedList<>();
        List<String> logs = new LinkedList<>();
        logs.add("\n");
        ConsistencyLevel consistencyLevel = clIterator.next();

        try {
            while (standardOutputs.hasNext()) {
                final String logLine = standardOutputs.next();
                if (pattern.matcher(logLine).find()) {
                    logs.add(logLine);
                    if (checkForConsistency(consistencyLevel, logLine)) {
                        founds.add(consistencyLevel);
                        if (clIterator.hasNext()) {
                            consistencyLevel = clIterator.next();
                        } else {
                            break;
                        }
                    }
                }
            }

            final String joinedLogs = Joiner.on("\n").join(logs);
            assertThat(founds).describedAs(joinedLogs + " expected consistency levels").isEqualTo(expectedConsistencyLevels);

        } finally {
            logStream = null;
            logger.setLevel(Level.WARN);
            logger.detachAppender(writerAppender);
            writerAppender.stop();
        }
    }

    protected boolean checkForConsistency(ConsistencyLevel consistencyLevel, String logLine) {
        return logLine.contains("cl=" + consistencyLevel.name()) || logLine.contains("at consistency " + consistencyLevel.name());
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
            logger.setLevel(Level.WARN);
            logger.detachAppender(writerAppender);
            writerAppender.stop();
        }
    }
}