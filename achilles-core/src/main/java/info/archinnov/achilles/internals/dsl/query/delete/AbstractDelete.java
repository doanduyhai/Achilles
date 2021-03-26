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

package info.archinnov.achilles.internals.dsl.query.delete;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import info.archinnov.achilles.internals.dsl.SchemaNameAware;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;

public abstract class AbstractDelete implements SchemaNameAware {

    protected final Delete.Selection delete;
    protected final RuntimeEngine rte;
    protected final List<Object> boundValues = new ArrayList<>();
    protected final List<Object> encodedValues = new ArrayList<>();

    protected AbstractDelete(RuntimeEngine rte) {
        this.delete = QueryBuilder.delete();
        this.rte = rte;
    }


}
