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

package info.archinnov.achilles.annotations;

import java.lang.annotation.*;

/**
 * Annotation for DSE Search.
 * <br/>
 * <br/>
 * Please note that this annotation is only valid for Cassandra versions:
 * <ul>
 *     <li><strong>DSE_4_8</strong></li>
 *     <li><strong>DSE_5_0_X</strong></li>
 * </ul>
 * <br/>
 * <br/>
 * <quote>
 *    Important: <strong>Achilles</strong> will NOT attempt to create the index for
 *    DSE Search even if <em>doForceSchemaCreation()</em> is set to <strong>true</strong>.
 *    You should create the index in DSE yourself using <em>dsetool create core ....</em> (please refer
 *    to DSE documentation)
 *    <br/>
 *    <br/>
 *    Nevertheless, <strong>Achilles</strong> will check the existence of DSE Search index at runtime
 *    and will complain if it cannot be found.
 *    <br/>
 *    <br/>
 *    Also, please note that currently OR clause is <strong>not yet supported</strong> by <strong>Achilles</strong>.
 *    Please use <em>...where().rawSolrQuery(String rawSolrQuery)</em> to search using OR clauses
 * </quote>
 * <br/>
 * <br/>
 * <quote>
 *     Additionally, you need not map the <strong>solr_query</strong> in your Java bean. Just put the @DSE_Search
 *     annotation on the fields you want to search and <strong>Achilles</strong> will generate the appropriate
 *     DSL source code.
 * </quote>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Documented
public @interface DSE_Search {

    /**
     * Only useful on a text/ascii field/column.
     * If enabled, <strong>Achilles</strong> will generate:
     * <ul>
     *     <li><em>StartWith(String prefix)</em></li>
     *     <li><em>EndWith(String suffix)</em></li>
     *     <li><em>Contain(String substring)</em></li>
     * </ul>
     *
     * methods in addition of the standard <em>Eq(String term)</em> and <em>RawPredicate(String rawSolrPredicate)</em> methods.
     *
     */
    boolean fullTextSearchEnabled() default false;

    String SOLR_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
}
