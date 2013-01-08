package fr.doan.achilles.entity.factory;

import static fr.doan.achilles.validation.Validator.validateNotEmpty;
import static fr.doan.achilles.validation.Validator.validateNotNull;

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

import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.dao.GenericWideRowDao;
import fr.doan.achilles.entity.manager.ThriftEntityManager;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ExternalWideMapProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.parser.EntityExplorer;
import fr.doan.achilles.entity.parser.EntityParser;
import fr.doan.achilles.exception.BeanMappingException;

public class ThriftEntityManagerFactoryImpl implements AchillesEntityManagerFactory
{

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
		validateNotNull(cluster, "cluster");
		validateNotNull(keyspace, "keyspace");
		validateNotEmpty(entityPackages, "entityPackages");
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.entityPackages = entityPackages;
		this.forceColumnFamilyCreation = forceCFCreation;
		this.columnFamilyHelper = new ColumnFamilyHelper(this.cluster, this.keyspace);
		this.bootstrap();
	}

	protected void bootstrap()
	{
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
								new GenericWideRowDao(keyspace, //
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

			throw new IllegalArgumentException(
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

	}

	@Override
	public boolean isOpen()
	{
		return true;
	}

}
