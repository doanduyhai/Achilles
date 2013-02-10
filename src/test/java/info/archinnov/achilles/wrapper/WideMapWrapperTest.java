package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static info.archinnov.achilles.serializer.SerializerUtils.INT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.holder.KeyValue;
import info.archinnov.achilles.holder.factory.KeyValueFactory;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.KeyValueIteratorForDynamicComposite;
import info.archinnov.achilles.iterator.factory.IteratorFactory;

import java.util.ArrayList;
import java.util.List;

import mapping.entity.UserBean;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
	private PropertyMeta<Integer, UserBean> joinWideMapMeta;

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
	private AchillesSliceIterator<Long, DynamicComposite, String> achillesSliceIterator;

	@Mock
	private AchillesJoinSliceIterator<Long, DynamicComposite, String, Integer, String> achillesJoinSliceIterator;

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
		String value = "test";
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert(wideMapMeta, 1)).thenReturn(composite);
		when(wideMapMeta.writeValueToString(value)).thenReturn(value);
		wrapper.insert(1, value);

		verify(dao).setValue(id, composite, value);

	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		String value = "test";
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert(wideMapMeta, 1)).thenReturn(composite);
		when(wideMapMeta.writeValueToString(value)).thenReturn(value);
		wrapper.insert(1, value, 12);

		verify(dao).setValue(id, composite, value, 12);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_keyvalue_asc_inclusive_start_exclusive_end() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<HColumn<DynamicComposite, String>> hColumns = mock(List.class);

		when(keyFactory.createForQuery(wideMapMeta, 1, true, 2, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});
		when(dao.findRawColumnsRange(id, start, end, false, 10)).thenReturn(hColumns);
		when(wideMapMeta.type()).thenReturn(WIDE_MAP);

		List<KeyValue<Integer, String>> result = new ArrayList<KeyValue<Integer, String>>();
		when(keyValueFactory.createKeyValueListForDynamicComposite(wideMapMeta, hColumns))
				.thenReturn(result);
		List<KeyValue<Integer, String>> expected = wrapper.find(1, true, 2, false, false, 10);

		assertThat(expected).isSameAs(result);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_join_keyvalue_asc_inclusive_start_exclusive_end() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<HColumn<DynamicComposite, String>> hColumns = mock(List.class);

		when(keyFactory.createForQuery(wideMapMeta, 1, true, 2, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});
		when(dao.findRawColumnsRange(id, start, end, false, 10)).thenReturn(hColumns);
		when(wideMapMeta.type()).thenReturn(JOIN_WIDE_MAP);

		List<KeyValue<Integer, String>> result = new ArrayList<KeyValue<Integer, String>>();
		when(keyValueFactory.createJoinKeyValueListForDynamicComposite(wideMapMeta, hColumns))
				.thenReturn(result);
		List<KeyValue<Integer, String>> expected = wrapper.find(1, true, 2, false, false, 10);

		assertThat(expected).isSameAs(result);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_values_asc_inclusive_start_exclusive_end() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<HColumn<DynamicComposite, String>> hColumns = mock(List.class);

		when(keyFactory.createForQuery(wideMapMeta, 1, true, 2, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(dao.findRawColumnsRange(id, start, end, false, 10)).thenReturn(hColumns);
		when(wideMapMeta.type()).thenReturn(WIDE_MAP);

		List<String> result = new ArrayList<String>();
		when(keyValueFactory.createValueListForDynamicComposite(wideMapMeta, hColumns)).thenReturn(
				result);

		List<String> expected = wrapper.findValues(1, true, 2, false, false, 10);

		assertThat(expected).isSameAs(result);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_join_values_asc_inclusive_start_exclusive_end() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<HColumn<DynamicComposite, String>> hColumns = mock(List.class);

		when(keyFactory.createForQuery(wideMapMeta, 1, true, 2, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(dao.findRawColumnsRange(id, start, end, false, 10)).thenReturn(hColumns);
		when(wideMapMeta.type()).thenReturn(JOIN_WIDE_MAP);

		List<String> result = new ArrayList<String>();
		when(keyValueFactory.createJoinValueListForDynamicComposite(wideMapMeta, hColumns))
				.thenReturn(result);

		List<String> expected = wrapper.findValues(1, true, 2, false, false, 10);

		assertThat(expected).isSameAs(result);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_keys_asc_inclusive_start_exclusive_end() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<HColumn<DynamicComposite, String>> hColumns = mock(List.class);

		when(keyFactory.createForQuery(wideMapMeta, 1, true, 2, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(dao.findRawColumnsRange(id, start, end, false, 10)).thenReturn(hColumns);
		when(wideMapMeta.type()).thenReturn(WIDE_MAP);

		List<Integer> result = new ArrayList<Integer>();
		when(keyValueFactory.createKeyListForDynamicComposite(wideMapMeta, hColumns)).thenReturn(
				result);

		List<Integer> expected = wrapper.findKeys(1, true, 2, false, false, 10);

		assertThat(expected).isSameAs(result);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_iterator_inclusive_start_exclusive_end() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createForQuery(wideMapMeta, 1, true, 2, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(wideMapMeta.type()).thenReturn(WIDE_MAP);
		when(dao.getColumnsIterator(id, start, end, false, 10)).thenReturn(achillesSliceIterator);

		KeyValueIteratorForDynamicComposite<Integer, String> iterator = mock(KeyValueIteratorForDynamicComposite.class);

		when(
				iteratorFactory.createKeyValueIteratorForDynamicComposite(achillesSliceIterator,
						wideMapMeta)).thenReturn(iterator);

		KeyValueIterator<Integer, String> expected = wrapper.iterator(1, true, 2, false, false, 10);

		assertThat(expected).isSameAs(iterator);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_join_iterator_inclusive_start_exclusive_end() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createForQuery(wideMapMeta, 1, true, 3, false, false)).thenReturn(
				new DynamicComposite[]
				{
						start,
						end
				});

		when(wideMapMeta.type()).thenReturn(JOIN_WIDE_MAP);
		when(dao.getJoinColumnsIterator(wideMapMeta, id, start, end, false, 10)).thenReturn(
				achillesJoinSliceIterator);

		KeyValueIteratorForDynamicComposite<Integer, String> iterator = mock(KeyValueIteratorForDynamicComposite.class);

		when(
				iteratorFactory.createKeyValueJoinIteratorForDynamicComposite(
						achillesJoinSliceIterator, wideMapMeta)).thenReturn(iterator);

		KeyValueIterator<Integer, String> expected = wrapper.iterator(1, true, 3, false, false, 10);

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
		wrapper.removeFirst(12);

		verify(dao).removeColumnRange(id, null, null, false, 12);
	}

	@Test
	public void should_remove_last() throws Exception
	{
		wrapper.removeLast(15);

		verify(dao).removeColumnRange(id, null, null, true, 15);
	}
}
