package info.archinnov.achilles.entity.factory;

import static info.archinnov.achilles.validation.Validator.validateNotEmpty;
import static info.archinnov.achilles.validation.Validator.validateNotNull;
import info.archinnov.achilles.columnFamily.ColumnFamilyHelper;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
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

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

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
	private ColumnFamilyHelper columnFamilyHelper;
	private ObjectMapperFactory objectMapperFactory = new DefaultObjectMapperFactory();

	protected ThriftEntityManagerFactoryImpl() {}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace, String entityPackages)
	{
		this(cluster, keyspace, entityPackages, false);
	}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			String entityPackages, ObjectMapperFactory factory)
	{
		this(cluster, keyspace, entityPackages, false, factory);
	}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			String entityPackages, ObjectMapper mapper)
	{
		this(cluster, keyspace, entityPackages, false, factoryFromMapper(mapper));
	}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			String entityPackages, boolean forceCFCreation)
	{
		this(cluster, keyspace, Arrays.asList(StringUtils.split(entityPackages, ",")),
				forceCFCreation, null);
	}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			String entityPackages, boolean forceCFCreation, ObjectMapperFactory factory)
	{
		this(cluster, keyspace, Arrays.asList(StringUtils.split(entityPackages, ",")),
				forceCFCreation, factory);
	}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			String entityPackages, boolean forceCFCreation, ObjectMapper mapper)
	{
		this(cluster, keyspace, Arrays.asList(StringUtils.split(entityPackages, ",")),
				forceCFCreation, factoryFromMapper(mapper));
	}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			List<String> entityPackages)
	{
		this(cluster, keyspace, entityPackages, false, null);
	}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			List<String> entityPackages, ObjectMapperFactory factory)
	{
		this(cluster, keyspace, entityPackages, false, factory);
	}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			List<String> entityPackages, ObjectMapper mapper)
	{
		this(cluster, keyspace, entityPackages, false, factoryFromMapper(mapper));
	}

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
		this.columnFamilyHelper = new ColumnFamilyHelper(this.cluster, this.keyspace);
		this.objectMapperFactory = factory != null ? factory : objectMapperFactory;
		this.entityParser = new EntityParser(this.objectMapperFactory);
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

		this.columnFamilyHelper.validateOrCreateColumnFamilies(this.entityMetaMap,
				this.forceColumnFamilyCreation);
	}

	protected void discoverEntities() throws ClassNotFoundException, IOException
	{
		log.info("Start discovery of entities searching in packages '{}'",
				StringUtils.join(entityPackages, ","));
		Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();

		List<Class<?>> classes = this.entityExplorer.discoverEntities(entityPackages);
		if (!classes.isEmpty())
		{

			for (Class<?> clazz : classes)
			{

				Pair<EntityMeta<?>, Map<PropertyMeta<?, ?>, Class<?>>> pair = entityParser
						.parseEntity(this.keyspace, clazz);
				entityMetaMap.put(clazz, pair.left);
				joinPropertyMetaToBeFilled.putAll(pair.right);
			}

			if (!joinPropertyMetaToBeFilled.isEmpty())
			{
				entityParser
						.fillJoinEntityMeta(keyspace, joinPropertyMetaToBeFilled, entityMetaMap);
			}

		}
		else
		{

			throw new BeanMappingException(
					"No entity with javax.persistence.Entity/javax.persistence.Table annotations found in the packages "
							+ StringUtils.join(entityPackages, ","));
		}
	}

	@Override
	public EntityManager createEntityManager()
	{
		return new ThriftEntityManager(entityMetaMap);
	}

	@Override
	public EntityManager createEntityManager(@SuppressWarnings("rawtypes") Map map)
	{
		return new ThriftEntityManager(entityMetaMap);
	}

	@Override
	public void close()
	{
		throw new UnsupportedOperationException("This operation is not supported for Cassandra");
	}

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
