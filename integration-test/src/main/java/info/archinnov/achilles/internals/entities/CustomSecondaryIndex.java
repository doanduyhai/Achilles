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

package info.archinnov.achilles.internals.entities;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.exceptions.ConfigurationException;

public class CustomSecondaryIndex extends SecondaryIndex {
    @Override
    public void init() {

    }

    @Override
    public void reload() {

    }

    @Override
    public void validateOptions() throws ConfigurationException {

    }

    @Override
    public String getIndexName() {
        return null;
    }

    @Override
    public String getNameForSystemKeyspace(ByteBuffer columnName) {
        return columnName.toString();
    }

    @Override
    protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
        return null;
    }

    @Override
    public void forceBlockingFlush() {

    }

    @Override
    public ColumnFamilyStore getIndexCfs() {
        return null;
    }

    @Override
    public void removeIndex(ByteBuffer columnName) {

    }

    @Override
    public void invalidate() {

    }

    @Override
    public void truncateBlocking(long truncatedAt) {

    }

    @Override
    public boolean indexes(CellName name) {
        return false;
    }

    @Override
    public boolean validate(ByteBuffer rowKey, Cell cell) {
        return false;
    }

    @Override
    public long estimateResultRows() {
        return 0;
    }
}
