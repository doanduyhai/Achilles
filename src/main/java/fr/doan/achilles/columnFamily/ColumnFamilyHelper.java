package fr.doan.achilles.columnFamily;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.exception.InvalidColumnFamilyException;
import fr.doan.achilles.validation.Validator;

public class ColumnFamilyHelper
{
	private Cluster cluster;
	private Keyspace keyspace;
	private ColumnFamilyBuilder columnFamilyBuilder = new ColumnFamilyBuilder();
	private ColumnFamilyValidator columnFamilyValidator = new ColumnFamilyValidator();

	public ColumnFamilyHelper(Cluster cluster, Keyspace keyspace) {
		this.cluster = cluster;
		this.keyspace = keyspace;
	}

	public ColumnFamilyDefinition discoverColumnFamily(String columnFamilyName)
	{
		KeyspaceDefinition keyspaceDef = this.cluster.describeKeyspace(this.keyspace
				.getKeyspaceName());
		if (keyspaceDef != null && keyspaceDef.getCfDefs() != null)
		{
			for (ColumnFamilyDefinition cfDef : keyspaceDef.getCfDefs())
			{
				if (StringUtils.equals(cfDef.getName(), columnFamilyName))
				{
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
		ColumnFamilyDefinition cfDef;
		if (entityMeta.isWideRow())
		{
			cfDef = this.columnFamilyBuilder.buildForWideRow(entityMeta,
					this.keyspace.getKeyspaceName());
		}
		else
		{
			cfDef = this.columnFamilyBuilder.buildForEntity(entityMeta,
					this.keyspace.getKeyspaceName());

		}
		this.addColumnFamily(cfDef);
	}

	public void validateColumnFamilies(Map<Class<?>, EntityMeta<?>> entityMetaMap,
			boolean forceColumnFamilyCreation)
	{
		for (Entry<Class<?>, EntityMeta<?>> entry : entityMetaMap.entrySet())
		{
			ColumnFamilyDefinition cfDef = this.discoverColumnFamily(entry.getValue()
					.getColumnFamilyName());
			if (cfDef == null)
			{
				if (forceColumnFamilyCreation)
				{
					this.createColumnFamily(entry.getValue());
				}
				else
				{
					throw new InvalidColumnFamilyException("The required column family '"
							+ entry.getValue().getColumnFamilyName()
							+ "' does not exist for entity '"
							+ entry.getValue().getCanonicalClassName() + "'");
				}
			}
			else
			{
				this.columnFamilyValidator.validate(cfDef, entry.getValue());
			}
		}
	}

	public static String normalizeCanonicalName(String canonicalName)
	{
		String newCanonicalName = canonicalName.replaceAll("\\.", "_").replaceAll("\\$", "_I_");
		if (newCanonicalName.length() < 48)
		{
			return newCanonicalName;
		}
		else
		{
			String packagaName = canonicalName.replaceAll("(.+)\\..+", "$1");
			String className = canonicalName.replaceAll(".+\\.(.+)", "$1");
			String firstPackage = canonicalName.replaceAll("^([a-zA-Z0-9]{2}).+$", "$1");
			Pattern pattern = Pattern.compile("\\.([a-zA-Z0-9]{2})");

			Matcher matcher = pattern.matcher(packagaName);

			List<String> shortPackages = new ArrayList<String>();
			shortPackages.add(firstPackage);
			while (matcher.find())
			{
				shortPackages.add(matcher.group(1));
			}

			String normalized = StringUtils.join(shortPackages, '_') + '_' + className;

			if (normalized.length() > 48)
			{
				normalized = className;
			}

			Validator
					.validateTrue(
							normalized.length() <= 48,
							"The column family '"
									+ normalized
									+ "' is too long. The maximum length for a column family name is 48 characters");

			return normalized;
		}
	}
}
