package fr.doan.achilles.wrapper;

import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.type.KeyValue;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.wrapper.factory.DynamicCompositeKeyFactory;

/**
 * WideMap
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapper<ID, K, V> extends AbstractWideMapWrapper<ID, K, V>
{

	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	@Override
	public List<KeyValue<K, V>> findRange(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		helper.checkBounds(start, end, reverse);

		DynamicComposite[] queryComps = keyFactory.createForQuery(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), start, inclusiveStart, end, inclusiveEnd, reverse);

		List<HColumn<DynamicComposite, Object>> hColumns = dao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		return KeyValue.fromListOfDynamicCompositeHColums(hColumns, wideMapMeta.getKeySerializer(),
				wideMapMeta);
	}

	@Override
	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		DynamicComposite[] queryComps = keyFactory.createForQuery(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), start, inclusiveStart, end, inclusiveEnd, reverse);

		ColumnSliceIterator<ID, DynamicComposite, Object> columnSliceIterator = dao
				.getColumnsIterator(id, queryComps[0], queryComps[1], reverse, count);

		return new KeyValueIterator(columnSliceIterator, wideMapMeta.getKeySerializer());
	}

	@Override
	public void removeRange(K start, boolean inclusiveStart, K end, boolean inclusiveEnd)
	{

		helper.checkBounds(start, end, false);

		DynamicComposite[] queryComps = keyFactory.createForQuery(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), start, inclusiveStart, end, inclusiveEnd, false);

		dao.removeColumnRange(id, queryComps[0], queryComps[1]);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	protected DynamicComposite buildComposite(K key)
	{
		return keyFactory.createForInsert(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), key, (Serializer) wideMapMeta.getKeySerializer());
	}

}
