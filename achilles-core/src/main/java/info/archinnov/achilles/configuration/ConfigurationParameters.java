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

package info.archinnov.achilles.configuration;

/**

 * Enum listing all configuration parameters
 * </p>
 * <hr>
 * <h4>Entity Management</h4>
 * <ul >
 * <li>
 * <strong>MANAGED_ENTITIES</strong> (OPTIONAL): list of entities to be managed by Achilles.
 * <br/>
 Example: <em>my.project.entity,another.project.entity</em></p>
 * </li>
 * </ul>
 * <br/>
 * <br/>
 * <h4>DDL</h4>
 * <ul >
 * <li>
 * <strong>FORCE_SCHEMA_GENERATION</strong> (OPTIONAL): create missing column families for entities if they are not found. <strong>Default = 'false'</strong>
 If set to <strong>false</strong> and no column family is found for any entity, <strong>Achilles</strong> will raise an <strong>AchillesInvalidColumnFamilyException</strong></p>
 * </li>
 * <li>
 * <strong>VALIDATE_SCHEMA</strong> (OPTIONAL): enable or disable schema validation at start-up. <strong>Default = 'true'</strong>
 * </li>
 * </ul>
 * <br/>
 * <br/>
 * <h4>DML</h4>
 * <ul>
 *     <li>
 *         <strong>DML_RESULTS_DISPLAY_SIZE</strong> (OPTIONAL): set the max number of returned rows to be displayed if ACHILLES_DML_STATEMENT logger or entity logger is debug-enabled
 *         .There is a <strong>hard-coded</strong> limit of 100 rows so if you provide a greater value it will be capped to 100 and floor to 0 (e.g. disable returned results display)
 *     </li>
 * </ul>
 * <br/>
 * <br/>
 * <h4>JSON Serialization</h4>
 * <ul >
 * <li>
 * <strong>JACKSON_MAPPER_FACTORY</strong> (OPTIONAL): an implementation of the <em>info.archinnov.achilles.json.JacksonMapperFactory</em> interface to build custom Jackson <strong>ObjectMapper</strong> based on entity class
 * </li>
 * <li>
 * <strong>JACKSON_MAPPER</strong> (OPTIONAL): default Jackson <strong>ObjectMapper</strong> to use for serializing entities
 * </li>
 * </ul>
 If both <strong>JACKSON_MAPPER_FACTORY</strong> and <strong>JACKSON_MAPPER</strong> parameters are provided, <strong>Achilles</strong> will ignore the <strong>JACKSON_MAPPER</strong> parameter and use <strong>JACKSON_MAPPER_FACTORY</strong></p>
 If none is provided, <strong>Achilles</strong> will use a default Jackson <strong>ObjectMapper</strong> with the following configuration:</p>
 * <ol >
 * <li>MapperFeature.SORT_PROPERTIES_ALPHABETICALLY = true </li>
 * <li>SerializationInclusion = JsonInclude.Include.NON_NULL </li>
 * <li>DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES = false </li>
 * <li>AnnotationIntrospector pair : primary = JacksonAnnotationIntrospector, secondary = JaxbAnnotationIntrospector </li>
 * </ol>
 * <br/>
 * <br/>
 * <h4>Consistency Level</h4>

 * <ul>
 * <li><strong>CONSISTENCY_LEVEL_READ_DEFAULT</strong> (OPTIONAL): default read consistency level for all entities</li>
 * <li><strong>CONSISTENCY_LEVEL_WRITE_DEFAULT</strong> (OPTIONAL): default write consistency level for all entities</li>
 * <li><strong>CONSISTENCY_LEVEL_SERIAL_DEFAULT</strong> (OPTIONAL): default serial consistency level for all entities</li>
 * <li><strong>CONSISTENCY_LEVEL_READ_MAP</strong> (OPTIONAL): map(String,String) of read consistency levels for column families/tables
 * <br/>
 * <br/>
 * Example:

 "columnFamily1" -&gt; "ONE" <br>
 * "columnFamily2" -&gt; "QUORUM"
 * ...
 * </p>
 * </li>
 * <li>
 * <strong>CONSISTENCY_LEVEL_WRITE_MAP</strong> (OPTIONAL): map(String,String) of write consistency levels for column families
 * Example:

 * "columnFamily1" -&gt; "ALL" <br>
 * "columnFamily2" -&gt; "EACH_QUORUM"
 * ...
 * </p>
 * </li>
 * <li>
 * <strong>CONSISTENCY_LEVEL_SERIAL_MAP</strong> (OPTIONAL): map(String,String) of write consistency levels for column families
 * Example:

 * "columnFamily1" -&gt; "SERIAL" <br>
 * "columnFamily2" -&gt; "LOCAL_SERIAL"
 * ...
 * </p>
 * </li>
 * </ul>
 * <br/>
 * <br/>
 * <h4><a name="user-content-events-interceptors"  href="#events-interceptors" ></a>Events Interceptors</h4>
 * <ul>
 * <li><strong>EVENT_INTERCEPTORS</strong> (OPTIONAL): list of events interceptors.</li>
 * </ul>
 * <br/>
 * <br/>
 * <h4>Bean Validation</h4>
 * <ul >
 * <li><strong>BEAN_VALIDATION_ENABLE</strong> (OPTIONAL): whether to enable Bean Validation for PRE_INSERT/PRE_UPDATE events. </li>
 * <li>
 * <strong>POST_LOAD_BEAN_VALIDATION_ENABLE</strong> (OPTIONAL): whether to enable Bean Validation for POST_LOAD event.
 * Please note that this flag is taken into account <strong>only if BEAN_VALIDATION_ENABLE is true</strong>
 * </li>
 * <li><strong>BEAN_VALIDATION_VALIDATOR</strong> (OPTIONAL): custom validator to be used.

 If no validator is provided, <strong>Achilles</strong> will get the default validator provided by the default Validation provider.
 * If Bean Validation is enabled at runtime but no default Validation provider can be found, an exception will be raised and the bootstrap is aborted</p>
 * </li>
 * </ul>
 * <br/>
 * <br/>
 * <h4>Prepared Statements Cache</h4>
 * <ul>
 * <li><strong>PREPARED_STATEMENTS_CACHE_SIZE</strong> (OPTIONAL): define the LRU cache size for prepared statements cache.

 By default, common operations like <code>insert</code>, <code>find</code> and <code>delete</code> are prepared before-hand for each entity class. For <code>update</code> and all operations with timestamp, since the updated fields and timestamp value are only known at runtime, <strong>Achilless</strong> will prepare the statements only on the fly and save them into a Guava LRU cache.</p>
 The default size is <code>10000</code> entries. Once the limit is reached, oldest prepared statements are evicted, causing <strong>Achilles</strong> to re-prepare them and get warnings from the Java Driver.</p>
 You can get details on the LRU cache state by putting the logger <code>info.archinnov.achilles.internal.statement.cache.CacheManager</code> on <strong>DEBUG</strong></p>
 * </li>
 * <li>
 * <strong>STATEMENTS_CACHE</strong> (OPTIONAL): provide an instance of the class {@link info.archinnov.achilles.internals.cache.StatementsCache}
 * to store all prepared statements. This option is useful for unit testing to avoid re-preparing many times the same prepared statements
 * <br/><br/>
 * <em>
 * Remark: if your provide the statement cache object yourself, the parameter PREPARED_STATEMENTS_CACHE_SIZE will be ignored
 * </em>
 * </li>
 * </ul>
 * <br/>
 * <br/>
 * <h4>Strategies</h4>
 * <ul>
 * <li>
 * <strong>GLOBAL_INSERT_STRATEGY</strong> (OPTIONAL): choose between <strong><code>InsertStrategy.ALL_FIELDS</code></strong> and <strong><code>InsertStrategy.NOT_NULL_FIELDS</code></strong>.
 Default value is <strong><code>ConfigurationParameters.InsertStrategy.ALL_FIELDS</code></strong>.</p>
 For more details, please check <strong><a href="https://github.com/doanduyhai/Achilles/wiki/Insert-Strategy">Insert Strategy</a></strong></p>
 * </li>
 * <li>
 * <strong>GLOBAL_NAMING_STRATEGY</strong> (OPTIONAL): choose between <strong><code>NamingStrategy.LOWER_CASE</code></strong>, <strong><code>NamingStrategy.SNAKE_CASE</code></strong> and <strong><code>NamingStrategy.CASE_SENSITIVE</code></strong>.
 Default value is <strong><code>NamingStrategy.LOWER_CASE</code></strong>.</p>
 For more details, please check <strong><a href="https://github.com/doanduyhai/Achilles/wiki/Entity-Mapping#naming-strategy">Naming Strategy</a></strong></p>
 * </li>
 * </ul>
 * <br/>
 * <br/>
 * <h4>Bean Factory</h4>
 * <br/>
 * <br/>
 * <ul>
 * <li>
 * <strong>DEFAULT_BEAN_FACTORY</strong> (OPTIONAL): inject the default bean factory to instantiate new entities and UDT classes.
 * The implementation class should implement the interface {@link info.archinnov.achilles.type.factory.BeanFactory}
 * The default implementation is straightforward
 * <pre class="code"><code class="java">
 * {@literal @}Override
 * public <T> T newInstance(Class<T> clazz) {
 * try{
 * return clazz.newInstance();
 * } catch (InstantiationException | IllegalAccessException e) {
 * ....
 * }
 * }
 * </code></pre>
 * </li>
 * </ul>
 * <br/>
 * <br/>
 * <h4>Schema Name Provider</h4>
 * <ul>
 * <li>
 * <strong>SCHEMA_NAME_PROVIDER</strong> (OPTIONAL): define a schema name provider to bind dynamically
 * an entity to a keyspace/table name at runtime. This feature is useful mostly in a multi-tenant context
 * </li>
 * </ul>
 * <br/>
 * <br/>
 * <h4>Asynchronous Operations</h4>
 * <ul>
 * <li><strong>EXECUTOR_SERVICE</strong> (OPTIONAL): define the executor service (thread pool) to be used by <strong>Achilles</strong> for its internal asynchronous operations.
 * By default, the thread pool is configured as follow:
 * <pre class="code"><code class="java">
 * new ThreadPoolExecutor(5, 20, 60, TimeUnit.SECONDS,
 * new LinkedBlockingQueue<Runnable>(1000),
 * new DefaultExecutorThreadFactory())
 * </code></pre>
 * </li>
 * <li>
 * <strong>DEFAULT_EXECUTOR_SERVICE_MIN_THREAD</strong> (OPTIONAL): define the minimum thread count for the executor service used by <strong>Achilles</strong> for its internal asynchronous operations.
 * The thread pool will configured as follow:
 * <pre class="code"><code class="java">
 * new ThreadPoolExecutor(DEFAULT_EXECUTOR_SERVICE_MIN_THREAD, 20, 60, TimeUnit.SECONDS,
 * new LinkedBlockingQueue<Runnable>(1000),
 * new DefaultExecutorThreadFactory())
 * </code></pre>
 * </li>
 * <li>
 * <strong>DEFAULT_EXECUTOR_SERVICE_MAX_THREAD</strong> (OPTIONAL): define the maximum thread count for the executor service used by <strong>Achilles</strong> for its internal asynchronous operations.
 * The thread pool will configured as follow:
 * <pre class="code"><code class="java">
 * new ThreadPoolExecutor(5, DEFAULT_EXECUTOR_SERVICE_MAX_THREAD, 60, TimeUnit.SECONDS,
 * new LinkedBlockingQueue<Runnable>(1000),
 * new DefaultExecutorThreadFactory())
 * </code></pre>
 * </li>
 * <li>
 * <strong>DEFAULT_EXECUTOR_SERVICE_THREAD_KEEPALIVE</strong> (OPTIONAL): define the duration in seconds during which a thread is kept alive before being destroyed, on the executor service used by <strong>Achilles</strong> for its internal asynchronous operations.
 * The thread pool will configured as follow:
 * <pre class="code"><code class="java">
 * new ThreadPoolExecutor(5, 20, DEFAULT_EXECUTOR_SERVICE_THREAD_KEEPALIVE, TimeUnit.SECONDS,
 * new LinkedBlockingQueue<Runnable>(1000),
 * new DefaultExecutorThreadFactory())
 * </code></pre>
 * </li>
 * <li>
 * <strong>DEFAULT_EXECUTOR_SERVICE_QUEUE_SIZE</strong> (OPTIONAL): define the size of the LinkedBlockingQueue used by the executor service used by <strong>Achilles</strong> for its internal asynchronous operations.
 * The thread pool will configured as follow:
 * <pre class="code"><code class="java">
 * new ThreadPoolExecutor(5, 20, 60, TimeUnit.SECONDS,
 * new LinkedBlockingQueue<Runnable>(DEFAULT_EXECUTOR_SERVICE_QUEUE_SIZE),
 * new DefaultExecutorThreadFactory())
 * </code></pre>
 * </li>
 * <li>
 * <strong>DEFAULT_EXECUTOR_SERVICE_THREAD_FACTORY</strong> (OPTIONAL): define the thread factory used by <strong>Achilles</strong> for its internal asynchronous operations.
 * The thread pool will configured as follow:
 * <pre class="code"><code class="java">
 * new ThreadPoolExecutor(5, 20, 60, TimeUnit.SECONDS,
 * new LinkedBlockingQueue<Runnable>(1000),
 * DEFAULT_EXECUTOR_SERVICE_THREAD_FACTORY)
 * </code></pre>
 * For more details, please check <strong><a href="https://github.com/doanduyhai/Achilles/wiki/Asynchronous-Operations">Asynchronous Operations</a></strong></p>
 * </li>
 * </ul>
 */
public enum ConfigurationParameters {
    NATIVE_SESSION("achilles.cassandra.native.session"),
    KEYSPACE_NAME("achilles.cassandra.keyspace.name"),

    JACKSON_MAPPER_FACTORY("achilles.json.jackson.mapper.factory"),
    JACKSON_MAPPER("achilles.json.jackson.mapper"),

    CONSISTENCY_LEVEL_READ_DEFAULT("achilles.consistency.read.default"),
    CONSISTENCY_LEVEL_WRITE_DEFAULT("achilles.consistency.write.default"),
    CONSISTENCY_LEVEL_SERIAL_DEFAULT("achilles.consistency.serial.default"),
    CONSISTENCY_LEVEL_READ_MAP("achilles.consistency.read.map"),
    CONSISTENCY_LEVEL_WRITE_MAP("achilles.consistency.write.map"),
    CONSISTENCY_LEVEL_SERIAL_MAP("achilles.consistency.serial.map"),

    EVENT_INTERCEPTORS("achilles.event.interceptors"),

    FORCE_SCHEMA_GENERATION("achilles.ddl.force.schema.generation"),

    VALIDATE_SCHEMA("achilles.validate.schema"),

    MANAGED_ENTITIES("achilles.managed.entities"),

    BEAN_VALIDATION_ENABLE("achilles.bean.validation.enable"),
    POST_LOAD_BEAN_VALIDATION_ENABLE("achilles.post.load.bean.validation.enable"),
    BEAN_VALIDATION_VALIDATOR("achilles.bean.validation.validator"),

    PREPARED_STATEMENTS_CACHE_SIZE("achilles.prepared.statements.cache.size"),

    DEFAULT_BEAN_FACTORY("achilles.bean.factory"),

    GLOBAL_INSERT_STRATEGY("achilles.global.insert.strategy"),
    GLOBAL_NAMING_STRATEGY("achilles.global.naming.strategy"),
    SCHEMA_NAME_PROVIDER("achilles.schema.name.provider"),

    EXECUTOR_SERVICE("achilles.executor.service"),
    STATEMENTS_CACHE("achilles.statements.cache"),

    RUNTIME_CODECS("achilles.runtime.codecs"),

    DEFAULT_EXECUTOR_SERVICE_MIN_THREAD("achilles.executor.service.default.thread.min"),
    DEFAULT_EXECUTOR_SERVICE_MAX_THREAD("achilles.executor.service.default.thread.max"),
    DEFAULT_EXECUTOR_SERVICE_THREAD_KEEPALIVE("achilles.executor.service.default.thread.keepalive"),
    DEFAULT_EXECUTOR_SERVICE_QUEUE_SIZE("achilles.executor.service.default.queue.size"),
    DEFAULT_EXECUTOR_SERVICE_THREAD_FACTORY("achilles.executor.service.thread.factory"),

    DML_RESULTS_DISPLAY_SIZE("achilles.dml.results_display.size");


    private String label;

    ConfigurationParameters(String label) {
        this.label = label;
    }

    /**
     * Small utility method that resolves a configuration based on its label.
     *
     * @param label the configuration label that would be populated in the map.
     * @return the label in ConfigurationParameters format, or null if no match found.
     */
    public static ConfigurationParameters fromLabel(String label) {
        for (ConfigurationParameters param : ConfigurationParameters.values()) {
            if (param.label.equals(label)) {
                return param;
            }
        }
        return null;
    }

}
