package info.archinnov.achilles.dao;

import static info.archinnov.achilles.entity.metadata.PropertyType.END_EAGER;
import static info.archinnov.achilles.entity.metadata.PropertyType.START_EAGER;
import static info.archinnov.achilles.serializer.SerializerUtils.DYNA_COMP_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GenericDynamicCompositeDao
 * 
 * @author DuyHai DOAN
 * 
 */
public class GenericDynamicCompositeDao<K> extends AbstractDao<K, DynamicComposite, String>
{
	private static final Logger log = LoggerFactory.getLogger(GenericDynamicCompositeDao.class);

	DynamicComposite startCompositeForEagerFetch;
	DynamicComposite endCompositeForEagerFetch;

	protected GenericDynamicCompositeDao() {
		this.initComposites();
	}

	public GenericDynamicCompositeDao(Keyspace keyspace, Serializer<K> keySrz, String cf) {
		super(keyspace);

		this.initComposites();
		keySerializer = keySrz;
		columnFamily = cf;
		columnNameSerializer = DYNA_COMP_SRZ;
		valueSerializer = STRING_SRZ;

		log.debug(
				"Initializing GenericDynamicCompositeDao for key serializer '{}', dynamic composite comparator and value serializer 'BytesType'",
				keySrz.getComparatorType().getTypeName());

	}

	public List<Pair<DynamicComposite, String>> eagerFetchEntity(K key)
	{
		log.trace("Eager fetching properties for column family {} ", columnFamily);

		return this.findColumnsRange(key, startCompositeForEagerFetch, endCompositeForEagerFetch,
				false, Integer.MAX_VALUE);
	}

	public Map<K, List<Pair<DynamicComposite, String>>> eagerFetchEntities(List<K> keys)
	{
		log.trace("Eager fetching properties for multiple entities in column family {} ",
				columnFamily);

		Map<K, List<Pair<DynamicComposite, String>>> map = new HashMap<K, List<Pair<DynamicComposite, String>>>();

		Rows<K, DynamicComposite, String> rows = this.multiGetSliceRange(keys,
				startCompositeForEagerFetch, endCompositeForEagerFetch, false, Integer.MAX_VALUE);

		for (Row<K, DynamicComposite, String> row : rows)
		{
			List<Pair<DynamicComposite, String>> columns = new ArrayList<Pair<DynamicComposite, String>>();
			for (HColumn<DynamicComposite, String> column : row.getColumnSlice().getColumns())
			{
				columns.add(new Pair<DynamicComposite, String>(column.getName(), column.getValue()));
			}

			map.put(row.getKey(), columns);
		}

		return map;
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
