package fr.doan.achilles.wrapper;

import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.metadata.WideMapMeta;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;
import fr.doan.achilles.iterator.DynamicCompositeKeyValueIterator;
import fr.doan.achilles.iterator.factory.IteratorFactory;
import fr.doan.achilles.serializer.Utils;
import fr.doan.achilles.wrapper.factory.DynamicCompositeKeyFactory;

/**
 * InternalWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class WideMapWrapperTest
{

	@InjectMocks
	private WideMapWrapper<Long, Integer, String> wrapper;

	@Mock
	private GenericEntityDao<Long> dao;

	@Mock
	private WideMapMeta<Integer, String> wideMapMeta;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	protected IteratorFactory iteratorFactory;

	@Mock
	private CompositeHelper helper;

	@Mock
	private ColumnSliceIterator<Long, DynamicComposite, Object> columnSliceIterator;

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Before
	public void setUp()
	{
		wrapper.setId(1L);

		when(wideMapMeta.getPropertyName()).thenReturn("name");
		when(wideMapMeta.propertyType()).thenReturn(WIDE_MAP);
		when(wideMapMeta.getKeySerializer()).thenReturn((Serializer) INT_SRZ);
	}

	@Test
	public void should_get_value() throws Exception
	{
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert("name", WIDE_MAP, 1, Utils.INT_SRZ)).thenReturn(composite);

		wrapper.get(1);

		verify(dao).getValue(1L, composite);

	}

	@Test
	public void should_insert_value() throws Exception
	{
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert("name", WIDE_MAP, 1, INT_SRZ)).thenReturn(composite);

		wrapper.insert(1, "test");

		verify(dao).setValue(1L, composite, "test");

	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert("name", WIDE_MAP, 1, INT_SRZ)).thenReturn(composite);

		wrapper.insert(1, "test", 12);

		verify(dao).setValue(1L, composite, "test", 12);

	}

	@SuppressWarnings(
	{
		"unchecked",
	})
	@Test
	public void should_find_values_asc() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<HColumn<DynamicComposite, Object>> hColumns = mock(List.class);

		when(keyFactory.createForQuery("name", WIDE_MAP, 1, true, 2, true, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(dao.findRawColumnsRange(1L, start, end, false, 10)).thenReturn(hColumns);
		List<KeyValue<Integer, String>> result = wrapper.findRange(1, 2, false, 10);

		assertThat(result).isNotNull();
		assertThat(result).isEmpty();
	}

	@SuppressWarnings(
	{
		"unchecked",
	})
	@Test
	public void should_find_values_asc_bounds_inclusive() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<HColumn<DynamicComposite, Object>> hColumns = mock(List.class);

		when(keyFactory.createForQuery("name", WIDE_MAP, 1, true, 2, true, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(dao.findRawColumnsRange(1L, start, end, false, 10)).thenReturn(hColumns);
		List<KeyValue<Integer, String>> result = wrapper.findRange(1, 2, true, false, 10);

		assertThat(result).isNotNull();
		assertThat(result).isEmpty();
	}

	@SuppressWarnings(
	{
		"unchecked",
	})
	@Test
	public void should_find_values_asc_bounds_exclusive() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<HColumn<DynamicComposite, Object>> hColumns = mock(List.class);

		when(keyFactory.createForQuery("name", WIDE_MAP, 1, false, 2, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});
		when(dao.findRawColumnsRange(1L, start, end, false, 10)).thenReturn(hColumns);
		List<KeyValue<Integer, String>> result = wrapper.findRange(1, 2, false, false, 10);

		assertThat(result).isNotNull();
		assertThat(result).isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_values_asc_inclusive_start_exclusive_end() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<HColumn<DynamicComposite, Object>> hColumns = mock(List.class);

		when(keyFactory.createForQuery("name", WIDE_MAP, 1, true, 2, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});
		when(dao.findRawColumnsRange(1L, start, end, false, 10)).thenReturn(hColumns);

		List<KeyValue<Integer, String>> result = wrapper.findRange(1, true, 2, false, false, 10);

		assertThat(result).isNotNull();
		assertThat(result).isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_iterator_default() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createForQuery("name", WIDE_MAP, 1, true, 2, true, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(dao.getColumnsIterator(1L, start, end, false, 10)).thenReturn(columnSliceIterator);

		DynamicCompositeKeyValueIterator<Integer, String> iterator = mock(DynamicCompositeKeyValueIterator.class);

		when(
				iteratorFactory.createDynamicCompositeKeyValueIterator(columnSliceIterator,
						INT_SRZ, wideMapMeta)).thenReturn(iterator);

		DynamicCompositeKeyValueIterator<Integer, String> expected = wrapper.iterator(1, 2, false,
				10);

		assertThat(expected).isSameAs(iterator);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_iterator_exclusive_bounds() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createForQuery("name", WIDE_MAP, 1, false, 3, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(dao.getColumnsIterator(1L, start, end, false, 10)).thenReturn(columnSliceIterator);

		DynamicCompositeKeyValueIterator<Integer, String> iterator = mock(DynamicCompositeKeyValueIterator.class);

		when(
				iteratorFactory.createDynamicCompositeKeyValueIterator(columnSliceIterator,
						INT_SRZ, wideMapMeta)).thenReturn(iterator);

		DynamicCompositeKeyValueIterator<Integer, String> expected = wrapper.iterator(1, 3, false,
				false, 10);

		assertThat(expected).isSameAs(iterator);

	}

	@Test
	public void should_remove() throws Exception
	{
		DynamicComposite comp = new DynamicComposite();
		when(keyFactory.createForInsert("name", WIDE_MAP, 5, INT_SRZ)).thenReturn(comp);

		wrapper.remove(5);

		verify(dao).removeColumn(1L, comp);
	}

	@Test
	public void should_remove_range() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createForQuery("name", WIDE_MAP, 5, true, 10, true, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});
		wrapper.removeRange(5, 10);

		verify(dao).removeColumnRange(1L, start, end);
	}
}
