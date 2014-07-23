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

package info.archinnov.achilles.configuration;

/**
 * <p>
 *  Enum listing all configuration parameters
 * </p>
 * <hr>
 * <h4>Entity Parsing</h4>
 * <ul >
 * <li>
 * <p><strong>ENTITY_PACKAGES</strong> (OPTIONAL): list of java packages for entity scanning, separated by comma.</p>
 *
 * <p>Example: <em>my.project.entity,another.project.entity</em> </p>
 * </li>
 * <li><p><strong>ENTITIES_LIST</strong> (OPTIONAL): list of entity classes for entity scanning.</p></li>
 * </ul><blockquote>
 * <p>Note: entities discovered by <strong>ENTITY_PACKAGES</strong>  will be merged into  entities provided by <strong>ENTITIES_LIST</strong></p>
 * </blockquote>
 *
 * <h4>DDL</h4>
 *
 * <ul >
 * <li>
 * <p><strong>FORCE_TABLE_CREATION</strong> (OPTIONAL): create missing column families for entities if they are not found. <strong>Default = 'false'</strong>.</p>
 *
 * <p>If set to <strong>false</strong> and no column family is found for any entity, <strong>Achilles</strong> will raise an <strong>AchillesInvalidColumnFamilyException</strong></p>
 * </li>
 * </ul>
 *
 * <h4>JSON Serialization</h4>
 *
 * <ul >
 * <li>
 * <strong>JACKSON_MAPPER_FACTORY</strong> (OPTIONAL): an implementation of the <em>info.archinnov.achilles.json.JacksonMapperFactory</em> interface to build custom Jackson <strong>ObjectMapper</strong> based on entity class</li>
 * <li>
 * <strong>JACKSON_MAPPER</strong> (OPTIONAL): default Jackson <strong>ObjectMapper</strong> to use for serializing entities</li>
 * </ul><p>If both <strong>JACKSON_MAPPER_FACTORY</strong> and <strong>JACKSON_MAPPER</strong> parameters are provided, <strong>Achilles</strong> will ignore the <strong>JACKSON_MAPPER</strong> parameter and use <strong>JACKSON_MAPPER_FACTORY</strong></p>
 *
 * <p>If none is provided, <strong>Achilles</strong> will use a default Jackson <strong>ObjectMapper</strong> with the following configuration:</p>
 *
 * <ol >
 * <li>SerializationInclusion = JsonInclude.Include.NON_NULL </li>
 * <li>DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES = false </li>
 * <li>AnnotationIntrospector pair : primary = JacksonAnnotationIntrospector, secondary = JaxbAnnotationIntrospector </li>
 * </ol>
 *
 * <h4>Consistency Level</h4>
 *
 * <ul >
 * <li>
 * <strong>CONSISTENCY_LEVEL_READ_DEFAULT</strong> (OPTIONAL): default read consistency level for all entities</li>
 * <li>
 * <strong>CONSISTENCY_LEVEL_WRITE_DEFAULT</strong> (OPTIONAL): default write consistency level for all entities</li>
 * <li>
 * <p><strong>CONSISTENCY_LEVEL_READ_MAP</strong> (OPTIONAL): map(String,String) of read consistency levels for column families/tables</p>
 *
 * <p>Example:</p>
 *
 * <p>"columnFamily1" -&gt; "ONE" <br>
 *   "columnFamily2" -&gt; "QUORUM"
 *   ...</p>
 * </li>
 * <li>
 * <p><strong>CONSISTENCY_LEVEL_WRITE_MAP</strong> (OPTIONAL): map(String,String) of write consistency levels for column families</p>
 *
 * <p>Example:</p>
 *
 * <p>"columnFamily1" -&gt; "ALL" <br>
 *   "columnFamily2" -&gt; "EACH_QUORUM"
 *   ...</p>
 * </li>
 * </ul><h4>
 * <a name="user-content-events-interceptors"  href="#events-interceptors" ></a>Events Interceptors</h4>
 *
 * <ul >
 * <li>
 * <strong>EVENT_INTERCEPTORS</strong> (OPTIONAL): list of events interceptors. </li>
 * </ul>
 *
 * <h4>Lossless Schema Update</h4>
 *
 * <ul >
 * <li>
 * <strong>ENABLE_SCHEMA_UPDATE</strong> (OPTIONAL): allow <code>ALTER TABLE xxx ADD</code> statements at runtime to add missing fields to existing tables in <strong>Cassandra</strong>
 * </li>
 * <li>
 * <strong>ENABLE_SCHEMA_UPDATE_FOR_TABLES</strong> (OPTIONAL): map of </li>
 * </ul><p>For more details on this feature, see <strong><a href="https://github.com/doanduyhai/Achilles/wiki/Dynamic-Schema-Update">Lossless Dynamic Schema Update</a></strong></p>
 *
 * <h4>Bean Validation</h4>
 *
 * <ul >
 * <li>
 * <strong>BEAN_VALIDATION_ENABLE</strong> (OPTIONAL): whether to enable Bean Validation. </li>
 * <li>
 * <p><strong>BEAN_VALIDATION_VALIDATOR</strong> (OPTIONAL): custom validator to be used. </p>
 *
 * <p>If no validator is provided, <strong>Achilles</strong> will get the default validator provided by the default Validation provider.
 * If Bean Validation is enabled at runtime but no default Validation provider can be found, an exception will be raised and the bootstrap is aborted</p>
 * </li>
 * </ul>
 *
 * <h4>Prepared Statements Cache</h4>
 *
 * <ul >
 * <li>
 * <strong>PREPARED_STATEMENTS_CACHE_SIZE</strong> (OPTIONAL): define the LRU cache size for prepared statements cache. </li>
 * </ul><p>By default, common operations like <code>insert</code>, <code>find</code> and <code>remove</code> are prepared before-hand for each entity class. For <code>update</code> and all operations with timestamp, since the updated fields and timestamp value are only known at runtime, <strong>Achilless</strong> will prepare the statements only on the fly and save them into a Guava LRU cache.</p>
 *
 * <p>The default size is <code>10000</code> entries. Once the limit is reached, oldest prepared statements are evicted, causing <strong>Achilles</strong> to re-prepare them and get warnings from the Java Driver.</p>
 *
 * <p>You can get details on the LRU cache state by putting the logger <code>info.archinnov.achilles.internal.statement.cache.CacheManager</code> on <strong>DEBUG</strong></p>
 *
 * <h4>Proxies</h4>
 *
 * <ul >
 * <li>
 * <strong>PROXIES_WARM_UP_DISABLED</strong> (OPTIONAL): disable <strong>CGLIB</strong> proxies warm-up. Default = <code>false</code>
 * </li>
 * </ul><p>The first time <strong>Achilles</strong> creates a proxy for an entity class, there is a penalty of a hundreds millisecs (value may change from different plateforms) required for <strong>CGLIB</strong> to read bytecode and add the proxy to its cache.</p>
 *
 * <p>This delay may be detrimental for real time applications that need very fast response-time.</p>
 *
 * <p>Therefore, at bootstrap time, <strong>Achilles</strong> will force proxy creation for each managed entities to warm up <strong>CGLIB</strong>. This behavior is enabled by default.</p>
 *
 * <p>If you want to speed up start up, you may disable this behavior. </p>
 *
 * <h4>Insert Strategy</h4>
 *
 * <ul >
 * <li>
 * <strong>INSERT_STRATEGY</strong> (OPTIONAL): choose between <strong><code>ConfigurationParameters.InsertStrategy.ALL_FIELDS</code></strong> and <strong><code>ConfigurationParameters.InsertStrategy.NOT_NULL_FIELDS</code></strong>.
 * Default value is <strong><code>ConfigurationParameters.InsertStrategy.ALL_FIELDS</code></strong>. </li>
 * </ul><p>For more details, please check <strong><a href="https://github.com/doanduyhai/Achilles/wiki/Insert-Strategy">Insert Strategy</a></strong></p>
 *
 * <h4>OSGI Class loader</h4>
 *
 * <ul >
 * <li>
 * <strong>OSGI_CLASS_LOADER</strong> (OPTIONAL): define the class loader to be use for entity introspection and proxies creation, instead of the default class loader.</li>
 * </ul><p>For more details, please check <strong><a href="https://github.com/doanduyhai/Achilles/wiki/OSGI-Support">OSGI Support</a></strong></p>
 *
 */
public enum ConfigurationParameters {
    ENTITY_PACKAGES("achilles.entity.packages"),
    ENTITIES_LIST("achilles.entities.list"),

    NATIVE_SESSION("achilles.cassandra.native.session"),
    KEYSPACE_NAME("achilles.cassandra.keyspace.name"),

    JACKSON_MAPPER_FACTORY("achilles.json.jackson.mapper.factory"),
    JACKSON_MAPPER("achilles.json.jackson.mapper"),

    CONSISTENCY_LEVEL_READ_DEFAULT("achilles.consistency.read.default"),
    CONSISTENCY_LEVEL_WRITE_DEFAULT("achilles.consistency.write.default"),
    CONSISTENCY_LEVEL_READ_MAP("achilles.consistency.read.map"),
    CONSISTENCY_LEVEL_WRITE_MAP("achilles.consistency.write.map"),

    EVENT_INTERCEPTORS("achilles.event.interceptors"),

    FORCE_TABLE_CREATION("achilles.ddl.force.table.creation"),

    ENABLE_SCHEMA_UPDATE("achilles.ddl.enable.schema.update"),
    ENABLE_SCHEMA_UPDATE_FOR_TABLES("achilles.ddl.enable.schema.update.for.tables"),

    BEAN_VALIDATION_ENABLE("achilles.bean.validation.enable"),
    BEAN_VALIDATION_VALIDATOR("achilles.bean.validation.validator"),

    PREPARED_STATEMENTS_CACHE_SIZE("achilles.prepared.statements.cache.size"),

    PROXIES_WARM_UP_DISABLED("achilles.proxies.warm.up.disabled"),

    INSERT_STRATEGY("achilles.insert.strategy"),

    OSGI_CLASS_LOADER("achilles.osgi.class.loader");

    private String label;

    ConfigurationParameters(String label) {
        this.label = label;
    }

}
