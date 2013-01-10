package fr.doan.achilles.columnFamily;

import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ExternalWideMapProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.exception.InvalidColumnFamilyException;

/**
 * ColumnFamilyHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ColumnFamilyHelper
{
	private static final Logger log = LoggerFactory.getLogger(ColumnFamilyHelper.class);

	private Cluster cluster;
	private Keyspace keyspace;
	private ColumnFamilyBuilder columnFamilyBuilder = new ColumnFamilyBuilder();
	private ColumnFamilyValidator columnFamilyValidator = new ColumnFamilyValidator();
	public static final Pattern CF_PATTERN = Pattern.compile("[a-zA-Z0-9_]{1,48}");

	public ColumnFamilyHelper(Cluster cluster, Keyspace keyspace) {
		this.cluster = cluster;
		this.keyspace = keyspace;
	}

	public ColumnFamilyDefinition discoverColumnFamily(String columnFamilyName)
	{
		log.debug("Start discovery of column family {}", columnFamilyName);

		KeyspaceDefinition keyspaceDef = this.cluster.describeKeyspace(this.keyspace
				.getKeyspaceName());
		if (keyspaceDef != null && keyspaceDef.getCfDefs() != null)
		{
			for (ColumnFamilyDefinition cfDef : keyspaceDef.getCfDefs())
			{
				if (StringUtils.equals(cfDef.getName(), columnFamilyName))
				{
					log.debug("Existing column family {} found", columnFamilyName);
					return cfDef;
				}
			}
		}
		return null;
	}

	public String addColumnFamily(ColumnFamilyDefinition cfDef)
	{
		return this.cluster.addColumnFamily(cfDef, true);
	}

	public void createColumnFamily(EntityMeta<?> entityMeta)
	{
		log.debug("Creating column family for entityMeta {}", entityMeta.getClassName());

		ColumnFamilyDefinition cfDef;
		if (entityMeta.isWideRow())
		{

			PropertyMeta<?, ?> propertyMeta = entityMeta.getPropertyMetas().values().iterator()
					.next();
			cfDef = this.columnFamilyBuilder.buildCompositeCF(this.keyspace.getKeyspaceName(),
					propertyMeta, entityMeta.getIdMeta().getValueClass(),
					entityMeta.getColumnFamilyName());
		}
		else
		{
			cfDef = this.columnFamilyBuilder.buildDynamicCompositeCF(entityMeta,
					this.keyspace.getKeyspaceName());

		}
		this.addColumnFamily(cfDef);
	}

	public void validateOrCreateColumnFamilies(Map<Class<?>, EntityMeta<?>> entityMetaMap,
			boolean forceColumnFamilyCreation)
	{
		for (Entry<Class<?>, EntityMeta<?>> entry : entityMetaMap.entrySet())
		{

			EntityMeta<?> entityMeta = entry.getValue();
			for (Entry<String, PropertyMeta<?, ?>> entryMeta : entityMeta.getPropertyMetas()
					.entrySet())
			{
				PropertyMeta<?, ?> propertyMeta = entryMeta.getValue();

				ExternalWideMapProperties<?> externalWideMapProperties = propertyMeta
						.getExternalWideMapProperties();
				if (externalWideMapProperties != null)
				{
					GenericCompositeDao<?, ?> externalWideMapDao = externalWideMapProperties
							.getExternalWideMapDao();
					this.validateOrCreateCFForExternalWideMap(propertyMeta, entityMeta.getIdMeta()
							.getValueClass(), forceColumnFamilyCreation, externalWideMapDao
							.getColumnFamily());
				}
			}

			this.validateOrCreateCFForEntity(entityMeta, forceColumnFamilyCreation);
		}
	}

	private <ID> void validateOrCreateCFForExternalWideMap(PropertyMeta<?, ?> propertyMeta,
			Class<ID> keyClass, boolean forceColumnFamilyCreation, String externalColumnFamilyName)
	{

		ColumnFamilyDefinition cfDef = this.discoverColumnFamily(externalColumnFamilyName);
		if (cfDef == null)
		{
			if (forceColumnFamilyCreation)
			{
				log.debug("Force creation of column family for propertyMeta {}",
						propertyMeta.getPropertyName());

				cfDef = this.columnFamilyBuilder.buildCompositeCF(this.keyspace.getKeyspaceName(),
						propertyMeta, keyClass, externalColumnFamilyName);
				this.cluster.addColumnFamily(cfDef, true);
			}
			else
			{
				throw new InvalidColumnFamilyException("The required column family '"
						+ externalColumnFamilyName + "' does not exist for field '"
						+ propertyMeta.getPropertyName() + "'");
			}
		}
		else
		{
			this.columnFamilyValidator.validateCFWithPropertyMeta(cfDef, propertyMeta,
					externalColumnFamilyName);
		}
	}

	public void validateOrCreateCFForEntity(EntityMeta<?> entityMeta,
			boolean forceColumnFamilyCreation)
	{
		ColumnFamilyDefinition cfDef = this.discoverColumnFamily(entityMeta.getColumnFamilyName());
		if (cfDef == null)
		{
			if (forceColumnFamilyCreation)
			{
				log.debug("Force creation of column family for entityMeta {}",
						entityMeta.getClassName());

				this.createColumnFamily(entityMeta);
			}
			else
			{
				throw new InvalidColumnFamilyException("The required column family '"
						+ entityMeta.getColumnFamilyName() + "' does not exist for entity '"
						+ entityMeta.getClassName() + "'");
			}
		}
		else
		{
			this.columnFamilyValidator.validateCFWithEntityMeta(cfDef, entityMeta);
		}
	}

	public static String normalizerAndValidateColumnFamilyName(String cfName)
	{
		log.trace("Normalizing column family '{}' name agains Cassandra restrictions", cfName);

		Matcher nameMatcher = CF_PATTERN.matcher(cfName);

		if (nameMatcher.matches())
		{
			return cfName;
		}
		else if (cfName.contains("."))
		{
			String className = cfName.replaceAll(".+\\.(.+)", "$1");
			return normalizerAndValidateColumnFamilyName(className);
		}
		else
		{
			throw new InvalidColumnFamilyException(
					"The column family name '"
							+ cfName
							+ "' is invalid. It should be respect the pattern [a-zA-Z0-9_] and be at most 48 characters long");
		}
	}
}
