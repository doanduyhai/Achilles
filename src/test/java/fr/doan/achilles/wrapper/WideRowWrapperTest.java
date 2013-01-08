package fr.doan.achilles.wrapper;

import static fr.doan.achilles.serializer.SerializerUtils.INT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
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

import fr.doan.achilles.composite.factory.CompositeKeyFactory;
import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;
import fr.doan.achilles.iterator.KeyValueIteratorForWideRow;
import fr.doan.achilles.iterator.factory.IteratorFactory;

/**
 * WideRowWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class WideRowWrapperTest
{
	@InjectMocks
	private WideRowWrapper<Long, Integer, String> wrapper;

	@Mock
	private GenericCompositeDao<Long, String> dao;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private CompositeHelper helper;

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
		ReflectionTestUtils.setField(wrapper, "helper", helper);
		ReflectionTestUtils.setField(wrapper, "keyValueFactory", keyValueFactory);
		ReflectionTestUtils.setField(wrapper, "iteratorFactory", iteratorFactory);
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
		when(wideMapMeta.getValue("test")).thenReturn("test");

		Object expected = wrapper.get(12);

		assertThat(expected).isEqualTo("test");
	}

	@Test
	public void should_insert_value() throws Exception
	{
		wrapper.insert(12, "test");
		verify(dao).setValue(id, comp, "test");
	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		wrapper.insert(12, "test", 452);
		verify(dao).setValue(id, comp, "test", 452);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_find_range() throws Exception
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
		when(
				keyValueFactory.createListForWideRowOrExternalWideMapMeta(wideMapMeta,
						(List) hColumns)).thenReturn(keyValues).thenReturn(keyValues);

		List<KeyValue<Integer, String>> expected = wrapper.findRange(12, 15, false, 10);
		assertThat(expected).isSameAs(keyValues);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_iterator() throws Exception
	{
		KeyValueIteratorForWideRow<Integer, String> keyValues = mock(KeyValueIteratorForWideRow.class);
		ColumnSliceIterator<Long, Composite, String> iterator = mock(ColumnSliceIterator.class);
		Composite startComp = new Composite();
		Composite endComp = new Composite();

		when(compositeKeyFactory.createForQuery(wideMapMeta, 12, true, 15, false, false)) //
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});
		when(dao.getColumnsIterator(id, startComp, endComp, false, 10)).thenReturn(iterator);
		when(iteratorFactory.createKeyValueIteratorForWideRow(iterator, wideMapMeta)).thenReturn(
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

		wrapper.removeRange(12, false, 15, true);

		verify(dao).removeColumnRange(id, startComp, endComp);
	}
}
