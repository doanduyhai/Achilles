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
package info.archinnov.achilles.query.cql;

import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.statement.wrapper.NativeQueryLog;
import info.archinnov.achilles.internal.statement.wrapper.NativeStatementWrapper;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.TypedMap;

/**
 * Class to wrap CQL3 native query
 *
 * <pre class="code"><code class="java">
 *
 *   String nativeQuery = "SELECT name,age_in_years FROM UserEntity WHERE id IN(?,?)";
 *   List<TypedMap> actual = manager.nativeQuery(nativeQuery,10L,11L).get();
 *
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Queries#native-query" target="_blank">Native query</a>
 */
public class NativeQuery {
    private static final Logger log = LoggerFactory.getLogger(NativeQuery.class);

    private DaoContext daoContext;

    private NativeQueryMapper mapper = new NativeQueryMapper();
    protected Object[] boundValues;

    protected NativeStatementWrapper nativeStatementWrapper;

    protected Options options;

    public NativeQuery(DaoContext daoContext, RegularStatement regularStatement, Options options, Object... boundValues) {
        this.daoContext = daoContext;
        this.nativeStatementWrapper = new NativeStatementWrapper(NativeQueryLog.class, regularStatement, boundValues, options.getCasResultListener());
        this.options = options;
        this.boundValues = boundValues;
    }

    /**
     * Return found rows. The list represents the number of returned rows The
     * map contains the (column name, column value) of each row. The map is
     * backed by a LinkedHashMap and thus preserves the columns order as they
     * were declared in the native query
     *
     * @return List<TypedMap>
     */
    public List<TypedMap> get() {
        log.debug("Get results for native query {}", nativeStatementWrapper.getStatement());
        List<Row> rows = daoContext.execute(nativeStatementWrapper).all();
        return mapper.mapRows(rows);
    }

    /**
     * Return the first found row. The map contains the (column name, column
     * value) of each row. The map is backed by a LinkedHashMap and thus
     * preserves the columns order as they were declared in the native query
     *
     * @return TypedMap
     */
    public TypedMap first() {
        log.debug("Get first result for native query {}", nativeStatementWrapper.getStatement());
        List<Row> rows = daoContext.execute(nativeStatementWrapper).all();
        List<TypedMap> result = mapper.mapRows(rows);
        if (result.isEmpty())
            return null;
        else
            return result.get(0);
    }

    /**
     * Execute statement without returning result. Useful for
     * INSERT/UPDATE/DELETE and DDL statements
     */
    public void execute() {
        log.debug("Execute native query {}", nativeStatementWrapper.getStatement());
        daoContext.execute(nativeStatementWrapper);
    }

    /**
     * Return an iterator of {@link info.archinnov.achilles.type.TypedMap} instance. Each instance represents a CQL row
     * @return Iterator<TypedMap>
     */
    public Iterator<TypedMap> iterator() {
        log.debug("Execute native query {} and return iterator", nativeStatementWrapper.getStatement());
        return new TypedMapIterator(daoContext.execute(nativeStatementWrapper).iterator());
    }
}
