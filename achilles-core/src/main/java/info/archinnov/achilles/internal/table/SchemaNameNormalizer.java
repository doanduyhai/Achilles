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
package info.archinnov.achilles.internal.table;

import info.archinnov.achilles.exception.AchillesInvalidTableException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaNameNormalizer {

	protected static final Logger log = LoggerFactory.getLogger(SchemaNameNormalizer.class);

	public static final Pattern CF_PATTERN = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9_]{1,47}|\"[a-zA-Z0-9][a-zA-Z0-9_]{1,47}\"");

    public static String extractTableNameFromCanonical(String canonical) {
        return canonical.replaceAll(".+\\.(.+)", "$1");
    }

    public static String validateSchemaName(String name) {
		log.trace("Normalizing schema name '{}' name against Cassandra restrictions", name);

		Matcher nameMatcher = CF_PATTERN.matcher(name);

		if (nameMatcher.matches()) {
			return name;
		} else {
			throw new AchillesInvalidTableException("The schema name '" + name
					+ "' is invalid. It should respect the pattern [a-zA-Z0-9][a-zA-Z0-9_]{1,47} optionally enclosed in double quotes (\")");
		}
	}

    public static boolean isCaseSensitive(String name) {
        return name != null && !name.equals(name.toLowerCase());
    }
}
