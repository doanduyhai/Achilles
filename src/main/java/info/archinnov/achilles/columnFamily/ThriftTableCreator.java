package info.archinnov.achilles.columnFamily;

import static info.archinnov.achilles.dao.ThriftCounterDao.COUNTER_CF;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesInvalidColumnFamilyException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ColumnFamilyHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftTableCreator extends AchillesTableCreator
{
	private static final Logger log = LoggerFactory.getLogger(ThriftTableCreator.class);
	private Cluster cluster;
	private Keyspace keyspace;
	private ThriftTableHelper thriftTableHelper = new ThriftTableHelper();
	private List<ColumnFamilyDefinition> cfDefs;
	public static final Pattern CF_PATTERN = Pattern.compile("[a-zA-Z0-9_]{1,48}");
	private Set<String> columnFamilyNames = new HashSet<String>();

	public ThriftTableCreator(Cluster cluster, Keyspace keyspace) {
		this.cluster = cluster;
		this.keyspace = keyspace;
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspace.getKeyspaceName());
		if (keyspaceDef != null && keyspaceDef.getCfDefs() != null)
		{
			cfDefs = keyspaceDef.getCfDefs();
		}
	}

	protected ColumnFamilyDefinition discoverColumnFamily(String columnFamilyName)
	{
		log.debug("Start discovery of column family {}", columnFamilyName);
		for (ColumnFamilyDefinition cfDef : this.cfDefs)
		{
			if (StringUtils.equals(cfDef.getName(), columnFamilyName))
			{
				log.debug("Existing column family {} found", columnFamilyName);
				return cfDef;
			}
		}
		return null;
	}

	protected void addColumnFamily(ColumnFamilyDefinition cfDef)
	{
		if (!columnFamilyNames.contains(cfDef.getName()))
		{
			columnFamilyNames.add(cfDef.getName());
			cluster.addColumnFamily(cfDef, true);
		}

	}

	protected void createColumnFamily(EntityMeta entityMeta)
	{
		log.debug("Creating column family for entityMeta {}", entityMeta.getClassName());
		String columnFamilyName = entityMeta.getColumnFamilyName();
		if (!columnFamilyNames.contains(columnFamilyName))
		{
			ColumnFamilyDefinition cfDef;
			if (entityMeta.isWideRow())
			{

				PropertyMeta<?, ?> propertyMeta = entityMeta
						.getPropertyMetas()
						.values()
						.iterator()
						.next();
				cfDef = thriftTableHelper.buildWideRowCF(keyspace.getKeyspaceName(), propertyMeta,
						entityMeta.getIdMeta().getValueClass(), columnFamilyName,
						entityMeta.getClassName());
			}
			else
			{
				cfDef = this.thriftTableHelper.buildEntityCF(entityMeta,
						this.keyspace.getKeyspaceName());

			}
			this.addColumnFamily(cfDef);
		}

	}

	@Override
	protected void validateOrCreateCFForWideMap(PropertyMeta<?, ?> propertyMeta, Class<?> keyClass,
			boolean forceColumnFamilyCreation, String externalColumnFamilyName, String entityName)
	{

		ColumnFamilyDefinition cfDef = discoverColumnFamily(externalColumnFamilyName);
		if (cfDef == null)
		{
			if (forceColumnFamilyCreation)
			{
				log.debug("Force creation of column family for propertyMeta {}",
						propertyMeta.getPropertyName());

				cfDef = thriftTableHelper.buildWideRowCF(keyspace.getKeyspaceName(), propertyMeta,
						keyClass, externalColumnFamilyName, entityName);
				this.addColumnFamily(cfDef);
			}
			else
			{
				throw new AchillesInvalidColumnFamilyException("The required column family '"
						+ externalColumnFamilyName + "' does not exist for field '"
						+ propertyMeta.getPropertyName() + "' of entity '"
						+ propertyMeta.getEntityClassName() + "'");
			}
		}
		else
		{
			this.thriftTableHelper.validateWideRowWithPropertyMeta(cfDef, propertyMeta,
					externalColumnFamilyName);
		}
	}

	@Override
	protected void validateOrCreateCFForEntity(EntityMeta entityMeta,
			boolean forceColumnFamilyCreation)
	{
		ColumnFamilyDefinition cfDef = this.discoverColumnFamily(entityMeta.getColumnFamilyName());
		if (cfDef == null)
		{
			if (forceColumnFamilyCreation)
			{
				log.debug("Force creation of column family for entityMeta {}",
						entityMeta.getClassName());

				createColumnFamily(entityMeta);
			}
			else
			{
				throw new AchillesInvalidColumnFamilyException("The required column family '"
						+ entityMeta.getColumnFamilyName() + "' does not exist for entity '"
						+ entityMeta.getClassName() + "'");
			}
		}
		else
		{
			this.thriftTableHelper.validateCFWithEntityMeta(cfDef, entityMeta);
		}
	}

	@Override
	protected void validateOrCreateCFForCounter(boolean forceColumnFamilyCreation)
	{
		ColumnFamilyDefinition cfDef = this.discoverColumnFamily(COUNTER_CF);
		if (cfDef == null)
		{
			if (forceColumnFamilyCreation)
			{
				log.debug("Force creation of column family for counters");

				this.createCounterColumnFamily();
			}
			else
			{
				throw new AchillesInvalidColumnFamilyException("The required column family '"
						+ COUNTER_CF + "' does not exist");
			}
		}
		else
		{
			this.thriftTableHelper.validateCounterCF(cfDef);
		}

	}

	private void createCounterColumnFamily()
	{
		log.debug("Creating generic counter column family");
		if (!columnFamilyNames.contains(COUNTER_CF))
		{
			ColumnFamilyDefinition cfDef = thriftTableHelper.buildCounterCF(this.keyspace
					.getKeyspaceName());
			this.addColumnFamily(cfDef);
		}
	}
}
