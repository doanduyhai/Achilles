package fr.doan.achilles.wrapper;

import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.serializer.SerializerUtils.INT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.GenericDynamicCompositeDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;
import fr.doan.achilles.iterator.AchillesSliceIterator;
import fr.doan.achilles.iterator.KeyValueIteratorForDynamicComposite;
import fr.doan.achilles.iterator.factory.IteratorFactory;

/**
 * WideMapWrapperTest
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
	private GenericDynamicCompositeDao<Long> dao;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	protected IteratorFactory iteratorFactory;

	@Mock
	private CompositeHelper helper;

	private Long id = 1L;

	@Mock
	private AchillesSliceIterator<Long, DynamicComposite, Object> achillesSliceIterator;

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Before
	public void setUp()
	{
		wrapper.setId(id);

		when(wideMapMeta.getPropertyName()).thenReturn("name");
		when(wideMapMeta.type()).thenReturn(WIDE_MAP);
		when(wideMapMeta.getKeySerializer()).thenReturn((Serializer) INT_SRZ);
	}

	@Test
	public void should_get_value() throws Exception
	{
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert(wideMapMeta, 1)).thenReturn(composite);

		wrapper.get(1);

		verify(dao).getValue(id, composite);

	}

	@Test
	public void should_insert_value() throws Exception
	{
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert(wideMapMeta, 1)).thenReturn(composite);

		wrapper.insert(1, "test");

		verify(dao).setValue(id, composite, "test");

	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert(wideMapMeta, 1)).thenReturn(composite);

		wrapper.insert(1, "test", 12);

		verify(dao).setValue(id, composite, "test", 12);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_values_asc() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<HColumn<DynamicComposite, Object>> hColumns = mock(List.class);

		when(keyFactory.createForQuery(wideMapMeta, 1, true, 2, true, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(dao.findRawColumnsRange(id, start, end, false, 10)).thenReturn(hColumns);
		List<KeyValue<Integer, String>> result = wrapper.find(1, 2, 10);

		assertThat(result).isNotNull();
		assertThat(result).isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_values_asc_bounds_exclusive() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<HColumn<DynamicComposite, Object>> hColumns = mock(List.class);

		when(keyFactory.createForQuery(wideMapMeta, 1, false, 2, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});
		when(dao.findRawColumnsRange(id, start, end, false, 10)).thenReturn(hColumns);
		List<KeyValue<Integer, String>> result = wrapper.findBoundsExclusive(1, 2, 10);

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

		when(keyFactory.createForQuery(wideMapMeta, 1, true, 2, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});
		when(dao.findRawColumnsRange(id, start, end, false, 10)).thenReturn(hColumns);

		List<KeyValue<Integer, String>> result = wrapper.find(1, true, 2, false, false, 10);

		assertThat(result).isNotNull();
		assertThat(result).isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_iterator_default() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createForQuery(wideMapMeta, 1, true, 2, true, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(dao.getColumnsIterator(id, start, end, false, 10)).thenReturn(achillesSliceIterator);

		KeyValueIteratorForDynamicComposite<Integer, String> iterator = mock(KeyValueIteratorForDynamicComposite.class);

		when(
				iteratorFactory.createKeyValueIteratorForDynamicComposite(achillesSliceIterator,
						wideMapMeta)).thenReturn(iterator);

		KeyValueIterator<Integer, String> expected = wrapper.iterator(1, 2, 10);

		assertThat(expected).isSameAs(iterator);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_iterator_exclusive_bounds() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createForQuery(wideMapMeta, 1, false, 3, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(dao.getColumnsIterator(id, start, end, false, 10)).thenReturn(achillesSliceIterator);

		KeyValueIteratorForDynamicComposite<Integer, String> iterator = mock(KeyValueIteratorForDynamicComposite.class);

		when(
				iteratorFactory.createKeyValueIteratorForDynamicComposite(achillesSliceIterator,
						wideMapMeta)).thenReturn(iterator);

		KeyValueIterator<Integer, String> expected = wrapper.iteratorBoundsExclusive(1, 3, 10);

		assertThat(expected).isSameAs(iterator);

	}

	@Test
	public void should_remove() throws Exception
	{
		DynamicComposite comp = new DynamicComposite();
		when(keyFactory.createForInsert(wideMapMeta, 5)).thenReturn(comp);

		wrapper.remove(5);

		verify(dao).removeColumn(id, comp);
	}

	@Test
	public void should_remove_range() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createForQuery(wideMapMeta, 5, true, 10, true, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});
		wrapper.remove(5, 10);

		verify(dao).removeColumnRange(id, start, end);
	}

	@Test
	public void should_remove_first() throws Exception
	{
		wrapper.removeFirst();

		verify(dao).removeColumnRange(id, null, null, false, 1);
	}

	@Test
	public void should_remove_last() throws Exception
	{
		wrapper.removeLast();

		verify(dao).removeColumnRange(id, null, null, true, 1);
	}
}
