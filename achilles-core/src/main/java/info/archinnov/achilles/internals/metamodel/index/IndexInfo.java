/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

package info.archinnov.achilles.internals.metamodel.index;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexInfo.class);

    public final IndexType type;
    public final String name;
    public final Optional<String> indexClassName;
    public final Optional<String> indexOptions;

    public IndexInfo(IndexType type, String name, String indexClassName, String indexOptions) {
        this.type = type;
        this.name = name;
        this.indexClassName = isNotBlank(indexClassName) ? Optional.ofNullable(indexClassName): Optional.empty();
        this.indexOptions = isNotBlank(indexOptions) ? Optional.ofNullable(indexClassName): Optional.empty();
    }

    public static IndexInfo noIndex() {
        return new IndexInfo(IndexType.NONE, null, null, null);
    }

    public String generate(Optional<String> keyspace, String table, String columnName) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generating index creation script for column %s in table %s of keyspace %s",
                    columnName, table, keyspace));
        }

        String template = "\nCREATE %s INDEX IF NOT EXISTS %s ON %s ( %s )%s%s;";
        String indexType;
        String tableWithKeyspace = table;
        String indexClassString = "";
        String custom = "";
        StringBuilder indexOptionsString = new StringBuilder("");

        switch (type) {
            case FULL:
                indexType = "FULL(" + columnName + ")";
                break;
            case MAP_ENTRY:
                indexType = "ENTRIES(" + columnName + ")";
                break;
            case MAP_KEY:
                indexType = "KEYS(" + columnName + ")";
                break;
            default:
                indexType = columnName;
        }

        if (keyspace.isPresent()) {
            tableWithKeyspace = keyspace.get() + "." + table;
        }

        if (indexClassName.isPresent()) {
            custom = "CUSTOM";
            indexClassString = " USING '" + indexClassName.get() + "'";
            indexOptions.ifPresent(x -> {
                if (isNotBlank(x)) {
                    indexOptionsString.append(" WITH OPTIONS = '").append(x).append("'");
                }
            });
        }
        return format(template, custom, name, tableWithKeyspace, indexType, indexClassString,
                indexOptionsString.toString());
    }
}
