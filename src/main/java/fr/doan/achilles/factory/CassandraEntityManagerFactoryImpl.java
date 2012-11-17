package fr.doan.achilles.factory;

import static fr.doan.achilles.validation.Validator.validateNotEmpty;
import static fr.doan.achilles.validation.Validator.validateNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.parser.EntityExplorer;
import fr.doan.achilles.parser.EntityParser;

public class CassandraEntityManagerFactoryImpl implements CassandraEntityManagerFactory
{

	private List<String> entityPackages;
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;
	private EntityParser entityParser = new EntityParser();
	private EntityExplorer entityExplorer = new EntityExplorer();
	private boolean forceColumnFamilyCreation = false;
	private Cluster cluster;
	private Keyspace keyspace;
	private ColumnFamilyHelper columnFamilyHelper;

	protected CassandraEntityManagerFactoryImpl() {}

	public CassandraEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace, String entityPackages) {
		this(cluster, keyspace, entityPackages, false);
	}

	public CassandraEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace, String entityPackages, boolean forceCFCreation) {
		this(cluster, keyspace, Arrays.asList(StringUtils.split(entityPackages, ",")), forceCFCreation);
	}

	public CassandraEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace, List<String> entityPackages) {
		this(cluster, keyspace, entityPackages, false);
	}

	public CassandraEntityManagerFactoryImpl(Cluster cluster, Keyspace keyspace, List<String> entityPackages, boolean forceCFCreation) {
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

	private void bootstrap()
	{
		this.discoverEntyties();

		this.columnFamilyHelper.validateColumnFamilies(this.entityMetaMap, this.forceColumnFamilyCreation);
	}

	private void discoverEntyties()
	{
		List<Class<?>> classes = this.entityExplorer.discoverEntities(entityPackages);

		if (classes != null)
		{

			for (Class<?> clazz : classes)
			{
				entityMetaMap.put(clazz, entityParser.parseEntity(clazz));
			}
		}
		else
		{

			throw new IllegalArgumentException("No entity with javax.persistence.Table annotation found in the packages "
					+ StringUtils.join(entityPackages, ","));
		}
	}

	@Override
	public EntityManager createEntityManager()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EntityManager createEntityManager(Map map)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isOpen()
	{
		return true;
	}

}
