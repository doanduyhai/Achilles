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

package info.archinnov.achilles.internals.statements;

import static info.archinnov.achilles.type.strategy.InsertStrategy.ALL_FIELDS;
import static java.lang.String.format;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.type.strategy.InsertStrategy;


public class BoundValuesWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(BoundValuesWrapper.class);

    public final List<BoundValueInfo> boundValuesInfo;
    public final AbstractEntityProperty<?> meta;

    public BoundValuesWrapper(AbstractEntityProperty<?> meta, List<BoundValueInfo> boundValuesInfo) {
        this.meta = meta;
        this.boundValuesInfo = boundValuesInfo;
    }

    public StatementWrapper bindWithInsertStrategy(PreparedStatement ps, InsertStrategy insertStrategy) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Bind values %s to query %s with insert strategy %s",
                    boundValuesInfo, ps.getQueryString(), insertStrategy.name()));
        }

        if (insertStrategy == ALL_FIELDS) {
            return new BoundStatementWrapper(OperationType.INSERT, meta, ps,
                    boundValuesInfo.stream().map(x -> x.boundValue).toArray(),
                    boundValuesInfo.stream().map(x -> x.encodedValue).toArray());
        } else {
            BoundStatement bs = ps.bind();
            boundValuesInfo.stream()
                    .filter(x -> x.encodedValue != null)
                    .forEach(x -> x.setter.accept(x.encodedValue, bs));
            return new BoundStatementWrapper(OperationType.INSERT, meta, bs,
                    boundValuesInfo.stream().map(x -> x.boundValue).toArray(),
                    boundValuesInfo.stream().map(x -> x.encodedValue).toArray());
        }
    }

    public StatementWrapper bindForUpdate(PreparedStatement ps) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Bind values %s to query %s for UPDATE",
                    boundValuesInfo, ps.getQueryString()));
        }

        BoundStatement bs = ps.bind();
        boundValuesInfo.stream()
                .filter(x -> x.encodedValue != null)
                .forEach(x -> x.setter.accept(x.encodedValue, bs));
        return new BoundStatementWrapper(OperationType.UPDATE, meta, bs,
                boundValuesInfo.stream().map(x -> x.boundValue).toArray(),
                boundValuesInfo.stream().map(x -> x.encodedValue).toArray());

    }
}
