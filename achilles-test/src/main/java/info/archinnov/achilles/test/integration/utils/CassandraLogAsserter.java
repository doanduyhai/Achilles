/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
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

public class CassandraLogAsserter {
	private static final String STORAGE_PROXY_LOGGER = "org.apache.cassandra.service.StorageProxy";
	private Logger storageProxyLogger = Logger.getLogger(STORAGE_PROXY_LOGGER);
	private WriterAppender writerAppender;
	private ByteArrayOutputStream logStream;

	public void prepareLogLevel() {
		logStream = new ByteArrayOutputStream();
		storageProxyLogger.setLevel(Level.TRACE);
		writerAppender = new WriterAppender();
		writerAppender.setWriter(new OutputStreamWriter(logStream));
		writerAppender.setLayout(new PatternLayout("%-5p [%d{ABSOLUTE}][%x] %c@:%M %m %n"));
		storageProxyLogger.addAppender(writerAppender);

	}

	public void assertConsistencyLevels(ConsistencyLevel read, ConsistencyLevel write) {
		String[] standardOutputs = StringUtils.split(logStream.toString(), "\n");
		try {
			for (String logLine : standardOutputs) {
				if (logLine.contains("fetchRows Command/ConsistencyLevel is")) {
					assertThat(logLine).contains("/" + read.name());
				}

				if (logLine.contains("mutate Mutations/ConsistencyLevel are [RowMutation")) {
					assertThat(logLine).contains("/" + write.name());
				}
			}
		} finally {
			logStream = null;
			storageProxyLogger.setLevel(Level.WARN);
			storageProxyLogger.removeAppender(writerAppender);
		}
	}
}
