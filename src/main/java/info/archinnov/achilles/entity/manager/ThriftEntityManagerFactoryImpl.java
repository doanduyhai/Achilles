package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.columnFamily.ColumnFamilyCreator;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.EntityExplorer;
import info.archinnov.achilles.entity.parser.EntityParser;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.validation.Validator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;
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
	private Cluster cluster;
	private Keyspace keyspace;
	private ColumnFamilyCreator columnFamilyCreator;
	private ObjectMapperFactory objectMapperFactory;

	private Map<Class<?>, EntityMeta<?>> entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
	private EntityParser entityParser;
	private EntityExplorer entityExplorer = new EntityExplorer();
	boolean forceColumnFamilyCreation = false;
	private CounterDao counterDao;

	private ArgumentExtractorForThriftEMF argumentExtractor = new ArgumentExtractorForThriftEMF();

	public static final ThreadLocal<Map<PropertyMeta<?, ?>, Class<?>>> joinPropertyMetaToBeFilledTL = new ThreadLocal<Map<PropertyMeta<?, ?>, Class<?>>>();
	public static final ThreadLocal<CounterDao> counterDaoTL = new ThreadLocal<CounterDao>();

	protected ThriftEntityManagerFactoryImpl() {
		this.counterDao = null;
	}

	/**
	 * Create a new ThriftEntityManagerFactoryImpl with a configuration map
	 * 
	 * @param configurationMap
	 * 
	 *            The configurationMap accepts the following properties:
	 * 
	 *            - achilles.entity.packages (MANDATORY): list of java packages for entity scanning, separated by comma. Example: my.project.entity,another.project.entity
	 * 
	 *            ------------------------------------------------------------------------------------------
	 * 
	 *            - achilles.cassandra.host: hostname/port of the Cassandra cluster Example: localhost:9160
	 * 
	 *            - achilles.cassandra.cluster.name: Cassandra cluster name
	 * 
	 *            - achilles.cassandra.keyspace.name: Cassandra keyspace name
	 * 
	 *            ------------------------------------------------------------------------------------------
	 * 
	 *            - achilles.cassandra.cluster: instance of pre-configured me.prettyprint.hector.api.Cluster object from Hector API
	 * 
	 *            - achilles.cassandra.keyspace: instance of pre-configured me.prettyprint.hector.api.Keyspace object from Hector API
	 * 
	 *            ------------------------------------------------------------------------------------------
	 * 
	 *            Either 'achilles.cassandra.cluster' or 'achilles.cassandra.host'/'achilles.cassandra.cluster.name' parameters should be provided
	 * 
	 *            Either 'achilles.cassandra.keyspace' or 'achilles.cassandra.keyspace.name' parameters should be provided
	 * 
	 *            ------------------------------------------------------------------------------------------
	 * 
	 *            - achilles.ddl.force.column.family.creation (OPTIONAL): create missing column families for entities if they are not found. Default = 'false'.
	 * 
	 *            If 'achilles.ddl.force.column.family.creation' = false and no column family is found for any entity, Achilles will raise an InvalidColumnFamilyException
	 * 
	 *            ------------------------------------------------------------------------------------------
	 * 
	 *            - achilles.json.object.mapper.factory (OPTIONAL): an implementation of the info.archinnov.achilles.json.ObjectMapperFactory interface to build custom Jackson ObjectMapper based on
	 *            entity class
	 * 
	 *            - achilles.json.object.mapper (OPTIONAL): default Jackson ObjectMapper to use for serializing entities
	 * 
	 *            If both 'achilles.json.object.mapper.factory' and 'achilles.json.object.mapper' parameters are provided, Achilles will ignore the 'achilles.json.object.mapper' value and use the
	 *            'achilles.json.object.mapper.factory'
	 * 
	 *            If none is provided, Achilles will use a default Jackson ObjectMapper with the following configuration:
	 * 
	 *            1. SerializationInclusion = Inclusion.NON_NULL
	 * 
	 *            2. DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES = false
	 * 
	 *            3. AnnotationIntrospector pair : primary = JacksonAnnotationIntrospector, secondary = JaxbAnnotationIntrospector
	 * 
	 */
	public ThriftEntityManagerFactoryImpl(Map<String, Object> configurationMap) {
		Validator.validateNotNull(configurationMap,
				"Configuration map for Achilles ThrifEntityManagerFactory should not be null");
		Validator.validateNotEmpty(configurationMap,
				"Configuration map for Achilles ThrifEntityManagerFactory should not be empty");

		this.entityPackages = argumentExtractor.initEntityPackages(configurationMap);
		this.cluster = argumentExtractor.initCluster(configurationMap);
		this.keyspace = argumentExtractor.initKeyspace(this.cluster, configurationMap);
		this.forceColumnFamilyCreation = argumentExtractor.initForceCFCreation(configurationMap);
		this.objectMapperFactory = argumentExtractor.initObjectMapperFactory(configurationMap);

		log.info(
				"Initializing Achilles ThriftEntityManagerFactory for cluster '{}' and keyspace '{}' ",
				cluster.getName(), keyspace.getKeyspaceName());

		this.columnFamilyCreator = new ColumnFamilyCreator(this.cluster, this.keyspace);
		this.entityParser = new EntityParser(this.objectMapperFactory);
		this.counterDao = new CounterDao(keyspace);
		this.bootstrap();
	}

	protected void bootstrap()
	{
		log.info("Bootstraping Achilles Thrift-based EntityManagerFactory ");
		boolean hasCounter;
		try
		{
			hasCounter = this.discoverEntities();
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
				this.forceColumnFamilyCreation, hasCounter);
	}

	protected boolean discoverEntities() throws ClassNotFoundException, IOException
	{
		log.info("Start discovery of entities searching in packages '{}'",
				StringUtils.join(entityPackages, ","));
		joinPropertyMetaToBeFilledTL.set(new HashMap<PropertyMeta<?, ?>, Class<?>>());
		counterDaoTL.set(counterDao);
		boolean hasCounter = false;
		List<Class<?>> classes = this.entityExplorer.discoverEntities(entityPackages);
		if (!classes.isEmpty())
		{

			for (Class<?> clazz : classes)
			{

				EntityMeta<?> entityMeta = entityParser.parseEntity(this.keyspace, clazz);
				entityMetaMap.put(clazz, entityMeta);
				if (entityMeta.hasCounter())
				{
					hasCounter = true;
				}
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
		return hasCounter;
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
}
