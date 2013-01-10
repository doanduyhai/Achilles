package fr.doan.achilles.dao;

import static fr.doan.achilles.entity.metadata.PropertyType.END_EAGER;
import static fr.doan.achilles.entity.metadata.PropertyType.START_EAGER;
import static fr.doan.achilles.serializer.SerializerUtils.DYNA_COMP_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.OBJECT_SRZ;

import java.util.List;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GenericDynamicCompositeDao
 * 
 * @author DuyHai DOAN
 * 
 */
public class GenericDynamicCompositeDao<K> extends AbstractDao<K, DynamicComposite, Object>
{
	private static final Logger log = LoggerFactory.getLogger(GenericDynamicCompositeDao.class);

	private DynamicComposite startCompositeForEagerFetch;
	private DynamicComposite endCompositeForEagerFetch;

	protected GenericDynamicCompositeDao() {
		this.initComposites();
	}

	public GenericDynamicCompositeDao(Keyspace keyspace, Serializer<K> keySrz, String cf) {
		super(keyspace);

		this.initComposites();
		keySerializer = keySrz;
		columnFamily = cf;
		columnNameSerializer = DYNA_COMP_SRZ;
		valueSerializer = OBJECT_SRZ;

		log.debug(
				"Initializing GenericDynamicCompositeDao for key serializer '{}', dynamic composite comparator and value serializer 'BytesType'",
				keySrz.getComparatorType().getTypeName());

	}

	public List<Pair<DynamicComposite, Object>> eagerFetchEntity(K key)
	{
		log.trace("Eager fetching properties for column family {} ", columnFamily);

		return this.findColumnsRange(key, startCompositeForEagerFetch, endCompositeForEagerFetch,
				false, Integer.MAX_VALUE);
	}

	private void initComposites()
	{
		startCompositeForEagerFetch = new DynamicComposite();
		startCompositeForEagerFetch.addComponent(0, START_EAGER.flag(), ComponentEquality.EQUAL);

		endCompositeForEagerFetch = new DynamicComposite();
		endCompositeForEagerFetch.addComponent(0, END_EAGER.flag(),
				ComponentEquality.GREATER_THAN_EQUAL);
	}
}
