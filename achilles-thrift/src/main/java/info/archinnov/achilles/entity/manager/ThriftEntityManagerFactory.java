package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.configuration.AchillesArgumentExtractor;
import info.archinnov.achilles.configuration.ThriftArgumentExtractor;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftDaoContextBuilder;
import info.archinnov.achilles.table.ThriftTableCreator;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Collections;
import java.util.Map;

import javax.persistence.EntityManager;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftEntityManagerFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityManagerFactory extends AchillesEntityManagerFactory
{

	private static final Logger log = LoggerFactory.getLogger(ThriftEntityManagerFactory.class);

	private Cluster cluster;
	private Keyspace keyspace;

	private ThriftDaoContext thriftDaoContext;

	/**
	 * Create a new ThriftEntityManagerFactoryImpl with a configuration map
	 * 
	 * @param configurationMap
	 * 
	 *            The configurationMap accepts the following properties:<br/>
	 *            <hr/>
	 *            <h1>Entity Packages</h1>
	 *            <br/>
	 *            <ul>
	 *            <li>"achilles.entity.packages" <strong>(MANDATORY)</strong>: list of java packages for entity scanning, separated by comma.<br/>
	 *            <br/>
	 *            &nbsp&nbspExample: <strong>"my.project.entity,another.project.entity"</strong></li>
	 *            </ul>
	 *            <hr/>
	 *            <h1>Cluster and Keyspace</h1>
	 *            <br/>
	 *            <ul>
	 *            <li>"achilles.cassandra.host": hostname/port of the Cassandra cluster <br/>
	 *            <br/>
	 *            &nbsp&nbspExample: <strong>localhost:9160</strong> <br/>
	 *            <br/>
	 *            </li>
	 *            <li>"achilles.cassandra.cluster.name": Cassandra cluster name</li>
	 *            <li>"achilles.cassandra.keyspace.name": Cassandra keyspace name</li>
	 *            </ul>
	 *            <ul>
	 *            <li>"achilles.cassandra.cluster": instance of pre-configured <em>me.prettyprint.hector.api.Cluster</em> object from Hector API</li>
	 *            <li>"achilles.cassandra.keyspace": instance of pre-configured <em>me.prettyprint.hector.api.Keyspace</em> object from Hector API</li>
	 *            </ul>
	 * 
	 *            <strong>Either "achilles.cassandra.cluster" or "achilles.cassandra.host"/"achilles.cassandra.cluster.name" parameters should be provided</strong> <br/>
	 *            <br/>
	 *            <strong>Either "achilles.cassandra.keyspace" or "achilles.cassandra.keyspace.name" parameters should be provided</strong>
	 * 
	 *            <hr/>
	 *            <h1>DDL Parameter</h1>
	 *            <br/>
	 *            <ul>
	 *            <li>"achilles.ddl.force.column.family.creation" <strong>(OPTIONAL)</strong>: create missing column families for entities if they are not found. Default = 'false'.<br/>
	 *            <br/>
	 *            &nbsp;&nbsp;If "achilles.ddl.force.column.family.creation" = false and no column family is found for any entity, Achilles will raise an
	 *            <strong>AchillesInvalidColumnFamilyException</strong><br/>
	 *            <br/>
	 *            </li>
	 *            </ul>
	 *            <hr/>
	 *            <h1>JSON Serialization</h1>
	 *            <br/>
	 *            <ul>
	 *            <li>"achilles.json.object.mapper.factory" <strong>(OPTIONAL)</strong>: an implementation of the <em>info.archinnov.achilles.json.ObjectMapperFactory</em> interface to build custom
	 *            <strong>Jackson ObjectMapper</strong> based on entity class<br/>
	 *            <br/>
	 *            </li>
	 *            <li>achilles.json.object.mapper <strong>(OPTIONAL)</strong>: default <strong>Jackson ObjectMapper</strong> to use for serializing entities<br/>
	 *            <br/>
	 *            &nbsp;&nbsp;If both "achilles.json.object.mapper.factory" and "achilles.json.object.mapper" parameters are provided, Achilles will ignore the "achilles.json.object.mapper" value and
	 *            use the "achilles.json.object.mapper.factory"<br/>
	 *            <br/>
	 *            If none is provided, Achilles will use a default <strong>Jackson ObjectMapper</strong> with the following configuration:<br/>
	 *            <br/>
	 *            <ol>
	 *            <li>SerializationInclusion = Inclusion.NON_NULL</li>
	 *            <li>DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES = false</li>
	 *            <li>AnnotationIntrospector pair : primary = <strong>JacksonAnnotationIntrospector</strong>, secondary = <strong>JaxbAnnotationIntrospector</strong></li>
	 *            </ol>
	 *            </li>
	 *            </ul>
	 *            <hr/>
	 *            <h1>Consistency Level</h1>
	 *            <br/>
	 *            <ul>
	 *            <li>"achilles.consistency.read.default" <strong>(OPTIONAL)</strong>: default read consistency level for all entities<br/>
	 *            <br/>
	 *            </li>
	 *            <li>"achilles.consistency.write.default" <strong>(OPTIONAL)</strong>: default write consistency level for all entities<br/>
	 *            <br/>
	 *            </li>
	 *            <li>"achilles.consistency.read.map" <strong>(OPTIONAL)</strong>: map(String,String) of read consistency levels for column families<br/>
	 *            <br/>
	 *            &nbsp;&nbsp;Example:<br/>
	 *            <br/>
	 *            &nbsp;&nbsp;&nbsp;&nbsp;"columnFamily1" -> "ONE"<br/>
	 *            &nbsp;&nbsp;&nbsp;&nbsp;"columnFamily2" -> "QUORUM"<br/>
	 *            &nbsp;&nbsp;&nbsp;&nbsp;...<br/>
	 *            <br/>
	 *            </li>
	 *            <li>"achilles.consistency.write.map" <strong>(OPTIONAL)</strong>: map(String,String) of write consistency levels for column families<br/>
	 *            <br/>
	 *            &nbsp;&nbsp;Example:<br/>
	 *            <br/>
	 *            &nbsp;&nbsp;&nbsp;&nbsp;"columnFamily1" -> "ALL"<br/>
	 *            &nbsp;&nbsp;&nbsp;&nbsp;"columnFamily2" -> "EACH_QUORUM"<br/>
	 *            &nbsp;&nbsp;&nbsp;&nbsp;...<br/>
	 *            <br/>
	 *            </li>
	 *            </ul>
	 *            <hr/>
	 *            <h1>Join consistency</h1>
	 *            <br/>
	 *            <ul>
	 *            <li>"achilles.consistency.join.check" <strong>(OPTIONAL)</strong>: whether check for join entity in Cassandra before inserting join primary key<br/>
	 *            <br/>
	 *            <strong>Warning: enable this option guarantees stronger data consistency but with the expense of read-before-write for each join entity insertion</strong> <br/>
	 *            </li>
	 *            </ul>
	 */
	public ThriftEntityManagerFactory(Map<String, Object> configurationMap) {
		super(configurationMap, new ThriftArgumentExtractor());

		ThriftArgumentExtractor thriftArgumentExtractor = new ThriftArgumentExtractor();
		cluster = thriftArgumentExtractor.initCluster(configurationMap);
		keyspace = thriftArgumentExtractor.initKeyspace(cluster,
				(ThriftConsistencyLevelPolicy) configContext.getConsistencyPolicy(),
				configurationMap);

		log
				.info("Initializing Achilles ThriftEntityManagerFactory for cluster '{}' and keyspace '{}' ",
						cluster.getName(), keyspace.getKeyspaceName());

		super.tableCreator = new ThriftTableCreator(cluster, keyspace);
		boolean hasSimpleCounter = bootstrap();
		thriftDaoContext = new ThriftDaoContextBuilder().buildDao(cluster, keyspace, entityMetaMap,
				configContext, hasSimpleCounter);
	}

	/**
	 * Create a new ThriftEntityManager
	 * 
	 * @return ThriftEntityManager
	 */
	@Override
	public EntityManager createEntityManager()
	{
		log.info("Create new Thrift-based Entity Manager ");

		return new ThriftEntityManager(this, Collections.unmodifiableMap(entityMetaMap), //
				thriftDaoContext, configContext);
	}

	/**
	 * Create a new ThriftEntityManager
	 * 
	 * @return ThriftEntityManager
	 */
	@Override
	public EntityManager createEntityManager(Map map)
	{
		log.info("Create new Thrift-based Entity Manager with properties");
		// TODO override configContext with provided properties
		return new ThriftEntityManager(this, Collections.unmodifiableMap(entityMetaMap),
				thriftDaoContext, configContext);
	}

	@Override
	protected AchillesConsistencyLevelPolicy initConsistencyLevelPolicy(
			Map<String, Object> configurationMap, AchillesArgumentExtractor argumentExtractor)
	{
		log.info("Initializing new Achilles Configurable Consistency Level Policy from arguments ");

		ConsistencyLevel defaultReadConsistencyLevel = argumentExtractor
				.initDefaultReadConsistencyLevel(configurationMap);
		ConsistencyLevel defaultWriteConsistencyLevel = argumentExtractor
				.initDefaultWriteConsistencyLevel(configurationMap);
		Map<String, ConsistencyLevel> readConsistencyMap = argumentExtractor
				.initReadConsistencyMap(configurationMap);
		Map<String, ConsistencyLevel> writeConsistencyMap = argumentExtractor
				.initWriteConsistencyMap(configurationMap);

		return new ThriftConsistencyLevelPolicy(defaultReadConsistencyLevel,
				defaultWriteConsistencyLevel, readConsistencyMap, writeConsistencyMap);
	}

	protected void setThriftDaoContext(ThriftDaoContext thriftDaoContext)
	{
		this.thriftDaoContext = thriftDaoContext;
	}

}
