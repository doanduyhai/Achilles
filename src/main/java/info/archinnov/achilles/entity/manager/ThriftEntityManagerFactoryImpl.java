package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.validation.Validator.validateNotEmpty;
import static info.archinnov.achilles.validation.Validator.validateNotNull;
import info.archinnov.achilles.columnFamily.ColumnFamilyCreator;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.EntityExplorer;
import info.archinnov.achilles.entity.parser.EntityParser;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.json.DefaultObjectMapperFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftEntityManagerFactoryImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityManagerFactoryImpl implements AchillesEntityManagerFactory
{

	private static final Logger log = LoggerFactory.getLogger(ThriftEntityManagerFactoryImpl.class);

	private List<String> entityPackages;
	private Map<Class<?>, EntityMeta<?>> entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
	private EntityParser entityParser;
	private EntityExplorer entityExplorer = new EntityExplorer();
	boolean forceColumnFamilyCreation = false;
	private Cluster cluster;
	private Keyspace keyspace;
	private ColumnFamilyCreator columnFamilyCreator;
	private ObjectMapperFactory objectMapperFactory = new DefaultObjectMapperFactory();
	private CounterDao counterDao;

	public static final ThreadLocal<Map<PropertyMeta<?, ?>, Class<?>>> joinPropertyMetaToBeFilledTL = new ThreadLocal<Map<PropertyMeta<?, ?>, Class<?>>>();
	public static final ThreadLocal<CounterDao> counterDaoTL = new ThreadLocal<CounterDao>();

	protected ThriftEntityManagerFactoryImpl() {
		this.counterDao = null;
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cluster
	 *            A me.prettyprint.hector.api.Cluster object from Hector API
	 * @param keyspace
	 *            A me.prettyprint.hector.api.Keyspace object from Hector API
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace, String entityPackages)
	{
		this(cluster, keyspace, entityPackages, false);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cassandraHost
	 *            Hostname and port to connect to a Cassandra cluster.
	 * 
	 *            Example: localhost:9160
	 * @param clusterName
	 *            The cluster name to connect to
	 * @param keyspaceName
	 *            The keyspace to use
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(String cassandraHost, String clusterName,
			String keyspaceName, String entityPackages)
	{
		this(cassandraHost, clusterName, keyspaceName, entityPackages, false);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cluster
	 *            A me.prettyprint.hector.api.Cluster object from Hector API
	 * @param keyspace
	 *            A me.prettyprint.hector.api.Keyspace object from Hector API
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param factory
	 *            An implementation of the info.archinnov.achilles.json.ObjectMapperFactory interface.
	 * 
	 *            This factory returns a Jackson ObjectMapper based on entity type
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			String entityPackages, ObjectMapperFactory factory)
	{
		this(cluster, keyspace, entityPackages, false, factory);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cassandraHost
	 *            Hostname and port to connect to a Cassandra cluster.
	 * 
	 *            Example: localhost:9160
	 * @param clusterName
	 *            The cluster name to connect to
	 * @param keyspaceName
	 *            The keyspace to use
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param factory
	 *            An implementation of the info.archinnov.achilles.json.ObjectMapperFactory interface.
	 * 
	 *            This factory returns a Jackson ObjectMapper based on entity type
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(String cassandraHost, String clusterName,
			String keyspaceName, String entityPackages, ObjectMapperFactory factory)
	{
		this(cassandraHost, clusterName, keyspaceName, entityPackages, false, factory);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cluster
	 *            A me.prettyprint.hector.api.Cluster object from Hector API
	 * @param keyspace
	 *            A me.prettyprint.hector.api.Keyspace object from Hector API
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param mapper
	 *            A Jackson ObjectMapper for JSON serialization of all entities
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			String entityPackages, ObjectMapper mapper)
	{
		this(cluster, keyspace, entityPackages, false, factoryFromMapper(mapper));
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cassandraHost
	 *            Hostname and port to connect to a Cassandra cluster.
	 * 
	 *            Example: localhost:9160
	 * @param clusterName
	 *            The cluster name to connect to
	 * @param keyspaceName
	 *            The keyspace to use
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param mapper
	 *            A Jackson ObjectMapper for JSON serialization of all entities
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(String cassandraHost, String clusterName,
			String keyspaceName, String entityPackages, ObjectMapper mapper)
	{
		this(cassandraHost, clusterName, keyspaceName, entityPackages, false,
				factoryFromMapper(mapper));
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cluster
	 *            A me.prettyprint.hector.api.Cluster object from Hector API
	 * @param keyspace
	 *            A me.prettyprint.hector.api.Keyspace object from Hector API
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param forceCFCreation
	 *            If true, Achilles will create the missing column family.
	 * 
	 *            In any case, Achilles check for the existence and validates existing column family for each entity
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			String entityPackages, boolean forceCFCreation)
	{
		this(cluster, keyspace, Arrays.asList(StringUtils.split(entityPackages, ",")),
				forceCFCreation, null);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cassandraHost
	 *            Hostname and port to connect to a Cassandra cluster.
	 * 
	 *            Example: localhost:9160
	 * @param clusterName
	 *            The cluster name to connect to
	 * @param keyspaceName
	 *            The keyspace to use
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param forceCFCreation
	 *            If true, Achilles will create the missing column family.
	 * 
	 *            In any case, Achilles check for the existence and validates existing column family for each entity
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(String cassandraHost, String clusterName,
			String keyspaceName, String entityPackages, boolean forceCFCreation)
	{
		this(cassandraHost, clusterName, keyspaceName, Arrays.asList(StringUtils.split(
				entityPackages, ",")), forceCFCreation, null);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cluster
	 *            A me.prettyprint.hector.api.Cluster object from Hector API
	 * @param keyspace
	 *            A me.prettyprint.hector.api.Keyspace object from Hector API
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param forceCFCreation
	 *            If true, Achilles will create the missing column family.
	 * 
	 *            In any case, Achilles check for the existence and validates existing column family for each entity
	 * @param factory
	 *            An implementation of the info.archinnov.achilles.json.ObjectMapperFactory interface.
	 * 
	 *            This factory returns a Jackson ObjectMapper based on entity type
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			String entityPackages, boolean forceCFCreation, ObjectMapperFactory factory)
	{
		this(cluster, keyspace, Arrays.asList(StringUtils.split(entityPackages, ",")),
				forceCFCreation, factory);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cassandraHost
	 *            Hostname and port to connect to a Cassandra cluster.
	 * 
	 *            Example: localhost:9160
	 * @param clusterName
	 *            The cluster name to connect to
	 * @param keyspaceName
	 *            The keyspace to use
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param forceCFCreation
	 *            If true, Achilles will create the missing column family.
	 * 
	 *            In any case, Achilles check for the existence and validates existing column family for each entity
	 * @param factory
	 *            An implementation of the info.archinnov.achilles.json.ObjectMapperFactory interface.
	 * 
	 *            This factory returns a Jackson ObjectMapper based on entity type
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(String cassandraHost, String clusterName,
			String keyspaceName, String entityPackages, boolean forceCFCreation,
			ObjectMapperFactory factory)
	{
		this(cassandraHost, clusterName, keyspaceName, Arrays.asList(StringUtils.split(
				entityPackages, ",")), forceCFCreation, factory);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cluster
	 *            A me.prettyprint.hector.api.Cluster object from Hector API
	 * @param keyspace
	 *            A me.prettyprint.hector.api.Keyspace object from Hector API
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param forceCFCreation
	 *            If true, Achilles will create the missing column family.
	 * 
	 *            In any case, Achilles check for the existence and validates existing column family for each entity
	 * @param mapper
	 *            A Jackson ObjectMapper for JSON serialization of all entities
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			String entityPackages, boolean forceCFCreation, ObjectMapper mapper)
	{
		this(cluster, keyspace, Arrays.asList(StringUtils.split(entityPackages, ",")),
				forceCFCreation, factoryFromMapper(mapper));
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cassandraHost
	 *            Hostname and port to connect to a Cassandra cluster.
	 * 
	 *            Example: localhost:9160
	 * @param clusterName
	 *            The cluster name to connect to
	 * @param keyspaceName
	 *            The keyspace to use
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param forceCFCreation
	 *            If true, Achilles will create the missing column family.
	 * 
	 *            In any case, Achilles check for the existence and validates existing column family for each entity
	 * @param mapper
	 *            A Jackson ObjectMapper for JSON serialization of all entities
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(String cassandraHost, String clusterName,
			String keyspaceName, String entityPackages, boolean forceCFCreation, ObjectMapper mapper)
	{
		this(cassandraHost, clusterName, keyspaceName, Arrays.asList(StringUtils.split(
				entityPackages, ",")), forceCFCreation, factoryFromMapper(mapper));
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cluster
	 *            A me.prettyprint.hector.api.Cluster object from Hector API
	 * @param keyspace
	 *            A me.prettyprint.hector.api.Keyspace object from Hector API
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			List<String> entityPackages)
	{
		this(cluster, keyspace, entityPackages, false, null);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cassandraHost
	 *            Hostname and port to connect to a Cassandra cluster.
	 * 
	 *            Example: localhost:9160
	 * @param clusterName
	 *            The cluster name to connect to
	 * @param keyspaceName
	 *            The keyspace to use
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(String cassandraHost, String clusterName,
			String keyspaceName, List<String> entityPackages)
	{
		this(cassandraHost, clusterName, keyspaceName, entityPackages, false, null);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cluster
	 *            A me.prettyprint.hector.api.Cluster object from Hector API
	 * @param keyspace
	 *            A me.prettyprint.hector.api.Keyspace object from Hector API
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param factory
	 *            An implementation of the info.archinnov.achilles.json.ObjectMapperFactory interface.
	 * 
	 *            This factory returns a Jackson ObjectMapper based on entity type
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			List<String> entityPackages, ObjectMapperFactory factory)
	{
		this(cluster, keyspace, entityPackages, false, factory);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cassandraHost
	 *            Hostname and port to connect to a Cassandra cluster.
	 * 
	 *            Example: localhost:9160
	 * @param clusterName
	 *            The cluster name to connect to
	 * @param keyspaceName
	 *            The keyspace to use
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param factory
	 *            An implementation of the info.archinnov.achilles.json.ObjectMapperFactory interface.
	 * 
	 *            This factory returns a Jackson ObjectMapper based on entity type
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(String cassandraHost, String clusterName,
			String keyspaceName, List<String> entityPackages, ObjectMapperFactory factory)
	{
		this(cassandraHost, clusterName, keyspaceName, entityPackages, false, factory);
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cluster
	 *            A me.prettyprint.hector.api.Cluster object from Hector API
	 * @param keyspace
	 *            A me.prettyprint.hector.api.Keyspace object from Hector API
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param mapper
	 *            A Jackson ObjectMapper for JSON serialization of all entities
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			List<String> entityPackages, ObjectMapper mapper)
	{
		this(cluster, keyspace, entityPackages, false, factoryFromMapper(mapper));
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cassandraHost
	 *            Hostname and port to connect to a Cassandra cluster.
	 * 
	 *            Example: localhost:9160
	 * @param clusterName
	 *            The cluster name to connect to
	 * @param keyspaceName
	 *            The keyspace to use
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param mapper
	 *            A Jackson ObjectMapper for JSON serialization of all entities
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(String cassandraHost, String clusterName,
			String keyspaceName, List<String> entityPackages, ObjectMapper mapper)
	{
		this(cassandraHost, clusterName, keyspaceName, entityPackages, false,
				factoryFromMapper(mapper));
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cluster
	 *            A me.prettyprint.hector.api.Cluster object from Hector API
	 * @param keyspace
	 *            A me.prettyprint.hector.api.Keyspace object from Hector API
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param forceCFCreation
	 *            If true, Achilles will create the missing column family.
	 * 
	 *            In any case, Achilles check for the existence and validates existing column family for each entity
	 * @param factory
	 *            An implementation of the info.archinnov.achilles.json.ObjectMapperFactory interface.
	 * 
	 *            This factory returns a Jackson ObjectMapper based on entity type
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			List<String> entityPackages, boolean forceCFCreation, ObjectMapperFactory factory)
	{
		log.info(
				"Initializing Achilles Thrift-based EntityManagerFactory for cluster '{}' and keyspace '{}' ",
				cluster.getName(), keyspace.getKeyspaceName());

		validateNotNull(cluster, "Cluster should not be null");
		validateNotNull(keyspace, "Keyspace should not be null");
		validateNotEmpty(entityPackages, "EntityPackages should not be empty");
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.entityPackages = entityPackages;
		this.forceColumnFamilyCreation = forceCFCreation;
		this.columnFamilyCreator = new ColumnFamilyCreator(this.cluster, this.keyspace);
		this.objectMapperFactory = factory != null ? factory : objectMapperFactory;
		this.entityParser = new EntityParser(this.objectMapperFactory);
		this.counterDao = new CounterDao(keyspace);
		this.bootstrap();
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl
	 * 
	 * @param cassandraHost
	 *            Hostname and port to connect to a Cassandra cluster.
	 * 
	 *            Example: localhost:9160
	 * @param clusterName
	 *            The cluster name to connect to
	 * @param keyspaceName
	 *            The keyspace to use
	 * @param entityPackages
	 *            List of packages separated by comma
	 * @param forceCFCreation
	 *            If true, Achilles will create the missing column family.
	 * 
	 *            In any case, Achilles check for the existence and validates existing column family for each entity
	 * @param factory
	 *            An implementation of the info.archinnov.achilles.json.ObjectMapperFactory interface.
	 * 
	 *            This factory returns a Jackson ObjectMapper based on entity type
	 * @return ThriftEntityManagerFactoryImpl
	 */
	public ThriftEntityManagerFactoryImpl(String cassandraHost, String clusterName,
			String keyspaceName, List<String> entityPackages, boolean forceCFCreation,
			ObjectMapperFactory factory)
	{
		log.info(
				"Initializing Achilles Thrift-based EntityManagerFactory for cassandra host {}, cluster '{}' and keyspace '{}' ",
				cassandraHost, clusterName, keyspaceName);

		validateNotNull(cassandraHost, "cassandraHost should not be null");
		validateNotNull(clusterName, "clusterName should not be null");
		validateNotNull(keyspaceName, "keyspaceName should not be null");
		validateNotEmpty(entityPackages, "EntityPackages should not be empty");

		this.cluster = HFactory.getOrCreateCluster(clusterName, new CassandraHostConfigurator(
				cassandraHost));
		this.keyspace = HFactory.createKeyspace(keyspaceName, cluster);
		this.entityPackages = entityPackages;
		this.forceColumnFamilyCreation = forceCFCreation;
		this.columnFamilyCreator = new ColumnFamilyCreator(this.cluster, this.keyspace);
		this.objectMapperFactory = factory != null ? factory : objectMapperFactory;
		this.entityParser = new EntityParser(this.objectMapperFactory);
		this.counterDao = new CounterDao(keyspace);
		this.bootstrap();
	}

	protected void bootstrap()
	{
		log.info("Bootstraping Achilles Thrift-based EntityManagerFactory ");

		try
		{
			this.discoverEntities();
		}
		catch (ClassNotFoundException e)
		{
			throw new RuntimeException(e);
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}

		this.columnFamilyCreator.validateOrCreateColumnFamilies(this.entityMetaMap,
				this.forceColumnFamilyCreation);
	}

	protected void discoverEntities() throws ClassNotFoundException, IOException
	{
		log.info("Start discovery of entities searching in packages '{}'",
				StringUtils.join(entityPackages, ","));
		joinPropertyMetaToBeFilledTL.set(new HashMap<PropertyMeta<?, ?>, Class<?>>());
		counterDaoTL.set(counterDao);

		List<Class<?>> classes = this.entityExplorer.discoverEntities(entityPackages);
		if (!classes.isEmpty())
		{

			for (Class<?> clazz : classes)
			{

				entityMetaMap.put(clazz, entityParser.parseEntity(this.keyspace, clazz));
			}

			Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = joinPropertyMetaToBeFilledTL
					.get();
			if (!joinPropertyMetaToBeFilled.isEmpty())
			{
				entityParser
						.fillJoinEntityMeta(keyspace, joinPropertyMetaToBeFilled, entityMetaMap);
			}

			joinPropertyMetaToBeFilledTL.remove();
			counterDaoTL.remove();
		}
		else
		{

			throw new BeanMappingException(
					"No entity with javax.persistence.Entity/javax.persistence.Table annotations found in the packages "
							+ StringUtils.join(entityPackages, ","));
		}
	}

	/**
	 * Create a new ThriftEntityManager
	 * 
	 * @return ThriftEntityManager
	 */
	@Override
	public EntityManager createEntityManager()
	{
		return new ThriftEntityManager(entityMetaMap);
	}

	/**
	 * Create a new ThriftEntityManager
	 * 
	 * @return ThriftEntityManager
	 */
	@Override
	public EntityManager createEntityManager(@SuppressWarnings("rawtypes") Map map)
	{
		return new ThriftEntityManager(entityMetaMap);
	}

	/**
	 * Not supported operation. Will throw UnsupportedOperationException
	 */
	@Override
	public void close()
	{
		throw new UnsupportedOperationException("This operation is not supported for Cassandra");
	}

	/**
	 * Not supported operation. Will throw UnsupportedOperationException
	 */
	@Override
	public boolean isOpen()
	{
		throw new UnsupportedOperationException("This operation is not supported for Cassandra");
	}

	protected static ObjectMapperFactory factoryFromMapper(final ObjectMapper mapper)
	{
		return new ObjectMapperFactory()
		{

			@Override
			public <T> ObjectMapper getMapper(Class<T> type)
			{
				return mapper;
			}
		};
	}
}
