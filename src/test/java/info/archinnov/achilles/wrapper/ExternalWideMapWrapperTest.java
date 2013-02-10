package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.serializer.SerializerUtils.INT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.holder.KeyValue;
import info.archinnov.achilles.holder.factory.KeyValueFactory;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.KeyValueIteratorForComposite;
import info.archinnov.achilles.iterator.factory.IteratorFactory;

import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * WideRowWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ExternalWideMapWrapperTest
{
	@InjectMocks
	private ExternalWideMapWrapper<Long, Integer, String> wrapper;

	@Mock
	private GenericCompositeDao<Long, String> dao;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private CompositeHelper compositeHelper;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	private IteratorFactory iteratorFactory;

	@Mock
	private CompositeKeyFactory compositeKeyFactory;

	private Long id;

	private Composite comp = new Composite();

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Before
	public void setUp()
	{
		wrapper.compositeHelper = compositeHelper;
		wrapper.keyValueFactory = keyValueFactory;
		wrapper.iteratorFactory = iteratorFactory;
		ReflectionTestUtils.setField(wrapper, "id", id);

		when(wideMapMeta.getKeySerializer()).thenReturn((Serializer) INT_SRZ);
		when(compositeKeyFactory.createBaseComposite(wideMapMeta, 12)).thenReturn(comp);
	}

	@Test
	public void should_get_value() throws Exception
	{

		Composite comp = new Composite();
		when(compositeKeyFactory.createBaseComposite(wideMapMeta, 12)).thenReturn(comp);
		when(dao.getValue(id, comp)).thenReturn("test");
		when(wideMapMeta.castValue("test")).thenReturn("test");

		Object expected = wrapper.get(12);

		assertThat(expected).isEqualTo("test");
	}

	@Test
	public void should_insert_value() throws Exception
	{
		when(wideMapMeta.writeValueAsSupportedTypeOrString("test")).thenReturn("test");
		wrapper.insert(12, "test");
		verify(dao).setValue(id, comp, "test");
	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		when(wideMapMeta.writeValueAsSupportedTypeOrString("test")).thenReturn("test");
		wrapper.insert(12, "test", 452);
		verify(dao).setValue(id, comp, "test", 452);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_find_keyvalues_range() throws Exception
	{
		List<HColumn<Composite, String>> hColumns = mock(List.class);
		List<KeyValue<Integer, String>> keyValues = mock(List.class);
		Composite startComp = new Composite();
		Composite endComp = new Composite();

		when(compositeKeyFactory.createForQuery(wideMapMeta, 12, true, 15, true, false)) //
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});

		when(dao.findRawColumnsRange(id, startComp, endComp, false, 10)).thenReturn(hColumns);
		when(keyValueFactory.createKeyValueListForComposite(wideMapMeta, (List) hColumns))
				.thenReturn(keyValues).thenReturn(keyValues);

		List<KeyValue<Integer, String>> expected = wrapper.find(12, 15, 10);
		assertThat(expected).isSameAs(keyValues);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_find_values_range() throws Exception
	{
		List<HColumn<Composite, String>> hColumns = mock(List.class);
		List<String> keyValues = mock(List.class);
		Composite startComp = new Composite();
		Composite endComp = new Composite();

		when(compositeKeyFactory.createForQuery(wideMapMeta, 12, true, 15, true, false)) //
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});

		when(dao.findRawColumnsRange(id, startComp, endComp, false, 10)).thenReturn(hColumns);
		when(keyValueFactory.createValueListForComposite(wideMapMeta, (List) hColumns)).thenReturn(
				keyValues).thenReturn(keyValues);

		List<String> expected = wrapper.findValues(12, 15, 10);
		assertThat(expected).isSameAs(keyValues);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_find_keys_range() throws Exception
	{
		List<HColumn<Composite, String>> hColumns = mock(List.class);
		List<Integer> keyValues = mock(List.class);
		Composite startComp = new Composite();
		Composite endComp = new Composite();

		when(compositeKeyFactory.createForQuery(wideMapMeta, 12, true, 15, true, false)) //
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});

		when(dao.findRawColumnsRange(id, startComp, endComp, false, 10)).thenReturn(hColumns);
		when(keyValueFactory.createKeyListForComposite(wideMapMeta, (List) hColumns)).thenReturn(
				keyValues).thenReturn(keyValues);

		List<Integer> expected = wrapper.findKeys(12, 15, 10);
		assertThat(expected).isSameAs(keyValues);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_iterator() throws Exception
	{
		KeyValueIteratorForComposite<Integer, String> keyValues = mock(KeyValueIteratorForComposite.class);
		AchillesSliceIterator<Long, Composite, String> iterator = mock(AchillesSliceIterator.class);
		Composite startComp = new Composite();
		Composite endComp = new Composite();

		when(compositeKeyFactory.createForQuery(wideMapMeta, 12, true, 15, false, false)) //
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});
		when(dao.getColumnsIterator(id, startComp, endComp, false, 10)).thenReturn(iterator);
		when(iteratorFactory.createKeyValueIteratorForComposite(iterator, wideMapMeta)).thenReturn(
				keyValues);
		KeyValueIterator<Integer, String> expected = wrapper.iterator(12, true, 15, false, false,
				10);

		assertThat(expected).isSameAs(keyValues);
	}

	@Test
	public void should_remove() throws Exception
	{
		Composite comp = new Composite();
		when(compositeKeyFactory.createBaseComposite(wideMapMeta, 12)).thenReturn(comp);

		wrapper.remove(12);

		verify(dao).removeColumn(id, comp);
	}

	@Test
	public void should_remove_range() throws Exception
	{
		Composite startComp = new Composite();
		Composite endComp = new Composite();

		when(compositeKeyFactory.createForQuery(wideMapMeta, 12, false, 15, true, false)) //
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});

		wrapper.remove(12, false, 15, true);

		verify(dao).removeColumnRange(id, startComp, endComp);
	}

	@Test
	public void should_remove_first() throws Exception
	{
		wrapper.removeFirst(3);

		verify(dao).removeColumnRange(id, null, null, false, 3);
	}

	@Test
	public void should_remove_last() throws Exception
	{
		wrapper.removeLast(7);

		verify(dao).removeColumnRange(id, null, null, true, 7);
	}
}
