package info.archinnov.achilles.entity.factory;

import static info.archinnov.achilles.validation.Validator.validateNotEmpty;
import static info.archinnov.achilles.validation.Validator.validateNotNull;

import info.archinnov.achilles.columnFamily.ColumnFamilyHelper;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.EntityExplorer;
import info.archinnov.achilles.entity.parser.EntityParser;
import info.archinnov.achilles.exception.BeanMappingException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
	private Map<Class<?>, EntityMeta<?>> entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
	private EntityParser entityParser = new EntityParser();
	private EntityExplorer entityExplorer = new EntityExplorer();
	private boolean forceColumnFamilyCreation = false;
	private Cluster cluster;
	private Keyspace keyspace;
	private ColumnFamilyHelper columnFamilyHelper;

	protected ThriftEntityManagerFactoryImpl() {}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace, String entityPackages)
	{
		this(cluster, keyspace, entityPackages, false);
	}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			String entityPackages, boolean forceCFCreation)
	{
		this(cluster, keyspace, Arrays.asList(StringUtils.split(entityPackages, ",")),
				forceCFCreation);
	}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			List<String> entityPackages)
	{
		this(cluster, keyspace, entityPackages, false);
	}

	public ThriftEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace,
			List<String> entityPackages, boolean forceCFCreation)
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
		this.bootstrap();
	}

	protected void bootstrap()
	{
		log.info("Bootstraping Achilles Thrift-based EntityManagerFactory ");

		Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();

		try
		{
			this.discoverEntities(joinPropertyMetaToBeFilled);
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

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	protected void discoverEntities(Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled)
			throws ClassNotFoundException, IOException
	{
		log.info("Start discovery of entities searching in packages '{}'",
				StringUtils.join(entityPackages, ","));

		List<Class<?>> classes = this.entityExplorer.discoverEntities(entityPackages);
		if (!classes.isEmpty())
		{

			for (Class<?> clazz : classes)
			{

				EntityMeta<?> entityMeta = entityParser.parseEntity(this.keyspace, clazz,
						joinPropertyMetaToBeFilled);
				entityMetaMap.put(clazz, entityMeta);
			}

			// Retrieve EntityMeta objects for join columns after entities parsing
			for (Entry<PropertyMeta<?, ?>, Class<?>> entry : joinPropertyMetaToBeFilled.entrySet())
			{
				Class<?> clazz = entry.getValue();
				if (entityMetaMap.containsKey(clazz))
				{
					PropertyMeta<?, ?> propertyMeta = entry.getKey();
					EntityMeta<?> joinEntityMeta = entityMetaMap.get(clazz);
					propertyMeta.getJoinProperties().setEntityMeta(joinEntityMeta);

					ExternalWideMapProperties<?> externalWideMapProperties = propertyMeta
							.getExternalWideMapProperties();

					if (externalWideMapProperties != null)
					{
						externalWideMapProperties.setExternalWideMapDao( //
								new GenericCompositeDao(keyspace, //
										externalWideMapProperties.getIdSerializer(), //
										joinEntityMeta.getIdSerializer(), //
										externalWideMapProperties.getExternalColumnFamilyName()));
					}

				}
				else
				{
					throw new BeanMappingException("Cannot find mapping for join entity '"
							+ clazz.getCanonicalName() + "'");
				}
			}
		}
		else
		{

			throw new BeanMappingException(
					"No entity with javax.persistence.Table annotation found in the packages "
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

}
