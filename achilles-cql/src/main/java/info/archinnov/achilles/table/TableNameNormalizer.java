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
package info.archinnov.achilles.table;

import info.archinnov.achilles.exception.AchillesInvalidTableException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableNameNormalizer {

	protected static final Logger log = LoggerFactory.getLogger(TableNameNormalizer.class);

	public static final Pattern CF_PATTERN = Pattern.compile("[a-zA-Z0-9_]{1,48}");

	public static String normalizerAndValidateColumnFamilyName(String cfName) {
		log.trace("Normalizing table '{}' name against Cassandra restrictions", cfName);

		Matcher nameMatcher = CF_PATTERN.matcher(cfName);

		if (nameMatcher.matches()) {
			return cfName;
		} else if (cfName.contains(".")) {
			String className = cfName.replaceAll(".+\\.(.+)", "$1");
			return normalizerAndValidateColumnFamilyName(className);
		} else {
			throw new AchillesInvalidTableException("The table name '" + cfName
					+ "' is invalid. It should be respect the pattern [a-zA-Z0-9_] and be at most 48 characters long");
		}
	}
}
