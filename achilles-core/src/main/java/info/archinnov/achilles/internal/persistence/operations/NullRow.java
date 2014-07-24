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

package info.archinnov.achilles.internal.persistence.operations;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;

public class NullRow implements Row {
    @Override
    public ColumnDefinitions getColumnDefinitions() {
        return null;
    }

    @Override
    public boolean isNull(int i) {
        return true;
    }

    @Override
    public boolean isNull(String name) {
        return true;
    }

    @Override
    public boolean getBool(int i) {
        return false;
    }

    @Override
    public boolean getBool(String name) {
        return false;
    }

    @Override
    public int getInt(int i) {
        return 0;
    }

    @Override
    public int getInt(String name) {
        return 0;
    }

    @Override
    public long getLong(int i) {
        return 0;
    }

    @Override
    public long getLong(String name) {
        return 0;
    }

    @Override
    public Date getDate(int i) {
        return null;
    }

    @Override
    public Date getDate(String name) {
        return null;
    }

    @Override
    public float getFloat(int i) {
        return 0;
    }

    @Override
    public float getFloat(String name) {
        return 0;
    }

    @Override
    public double getDouble(int i) {
        return 0;
    }

    @Override
    public double getDouble(String name) {
        return 0;
    }

    @Override
    public ByteBuffer getBytesUnsafe(int i) {
        return null;
    }

    @Override
    public ByteBuffer getBytesUnsafe(String name) {
        return null;
    }

    @Override
    public ByteBuffer getBytes(int i) {
        return null;
    }

    @Override
    public ByteBuffer getBytes(String name) {
        return null;
    }

    @Override
    public String getString(int i) {
        return null;
    }

    @Override
    public String getString(String name) {
        return null;
    }

    @Override
    public BigInteger getVarint(int i) {
        return null;
    }

    @Override
    public BigInteger getVarint(String name) {
        return null;
    }

    @Override
    public BigDecimal getDecimal(int i) {
        return null;
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return null;
    }

    @Override
    public UUID getUUID(int i) {
        return null;
    }

    @Override
    public UUID getUUID(String name) {
        return null;
    }

    @Override
    public InetAddress getInet(int i) {
        return null;
    }

    @Override
    public InetAddress getInet(String name) {
        return null;
    }

    @Override
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        return null;
    }

    @Override
    public <T> List<T> getList(String name, Class<T> elementsClass) {
        return null;
    }

    @Override
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        return null;
    }

    @Override
    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        return null;
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        return null;
    }

    @Override
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return null;
    }
}
