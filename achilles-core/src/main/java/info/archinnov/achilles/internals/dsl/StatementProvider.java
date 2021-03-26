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

package info.archinnov.achilles.internals.dsl;


import java.util.List;

import com.datastax.driver.core.BoundStatement;

public interface StatementProvider {

    /**
     * <ul>
     * <li>1. generate the statement</li>
     * <li>2. prepare the statement and put it to the Statements Cache</li>
     * <li>3a. bind the prepared statement with given values OR</li>
     * <li>3b. extract values from a given entity and bind the prepared statement</li>
     * <li>4. return the bound statement</li>
     * </ul>
     *
     * @return BoundStatement
     */
    BoundStatement generateAndGetBoundStatement();

    /**
     * Generate the statement as plain text
     *
     * @return plain text statement
     */
    String getStatementAsString();

    /**
     * Return the provided raw (not yet encoded) bound values
     * OR extract the raw bound values from a given entity
     *
     * @return list of raw bound values
     */
    List<Object> getBoundValues();

    /**
     * Return the provided encoded bound values
     * OR extract the raw bound values from a given entity
     * and encode them
     *
     * @return list of encoded bound values
     */
    List<Object> getEncodedBoundValues();
}
