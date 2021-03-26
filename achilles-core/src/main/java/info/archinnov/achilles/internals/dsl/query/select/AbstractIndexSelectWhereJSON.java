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

package info.archinnov.achilles.internals.dsl.query.select;

import static java.lang.String.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.querybuilder.Select;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.statements.BoundStatementWrapper;
import info.archinnov.achilles.internals.statements.OperationType;
import info.archinnov.achilles.internals.statements.StatementWrapper;

public abstract class AbstractIndexSelectWhereJSON<T extends AbstractIndexSelectWhereJSON<T, ENTITY>, ENTITY>
        extends AbstractSelectWhereJSON<T, ENTITY> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIndexSelectWhereJSON.class);

    protected AbstractIndexSelectWhereJSON(Select.Where where, CassandraOptions cassandraOptions) {
        super(where, cassandraOptions);
    }

    @Override
    protected StatementWrapper getInternalBoundStatementWrapper() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Get bound statement wrapper"));
        }

        final RuntimeEngine rte = getRte();
        final AbstractEntityProperty<ENTITY> meta = getMetaInternal();
        final CassandraOptions cassandraOptions = getOptions();

        final String queryString;
        if (cassandraOptions.hasRawSolrQuery()) {
            getBoundValuesInternal().add(0, cassandraOptions.generateRawSolrQuery());
            getEncodedValuesInternal().add(0, cassandraOptions.generateRawSolrQuery());
            queryString = where.getQueryString();
        } else if (cassandraOptions.hasSolrQuery()) {
            getBoundValuesInternal().add(0, cassandraOptions.generateSolrQuery());
            getEncodedValuesInternal().add(0, cassandraOptions.generateSolrQuery());
            queryString = where.getQueryString();
        } else {
            queryString = where.getQueryString().trim().replaceFirst(";$", " ALLOW FILTERING;");
        }

        final PreparedStatement ps = rte.prepareDynamicQuery(queryString);

        final StatementWrapper statementWrapper = new BoundStatementWrapper(OperationType.SELECT,
                meta, ps,
                getBoundValuesInternal().toArray(),
                getEncodedValuesInternal().toArray());

        statementWrapper.applyOptions(cassandraOptions);
        return statementWrapper;
    }
}
