package info.archinnov.achilles.proxy.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.ThriftPropertyHelper;
import info.archinnov.achilles.iterator.ThriftKeyValueIteratorImpl;
import info.archinnov.achilles.iterator.ThriftSliceIterator;
import info.archinnov.achilles.iterator.factory.ThriftIteratorFactory;
import info.archinnov.achilles.iterator.factory.ThriftKeyValueFactory;
import info.archinnov.achilles.proxy.AchillesEntityInterceptor;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;
import info.archinnov.achilles.type.WideMap.BoundingMode;
import info.archinnov.achilles.type.WideMap.OrderingMode;

import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * ThriftWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftWideMapWrapperTest
{
	@InjectMocks
	private ThriftWideMapWrapper<Integer, String> wrapper;

	@Mock
	private ThriftPersistenceContext context;

	@Mock
	private ThriftGenericWideRowDao dao;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private ThriftPropertyHelper thriftPropertyHelper;

	@Mock
	private ThriftKeyValueFactory thriftKeyValueFactory;

	@Mock
	private ThriftIteratorFactory thriftIteratorFactory;

	@Mock
	private ThriftCompositeFactory thriftCompositeFactory;

	private Long id;

	private Composite comp = new Composite();

	@Mock
	private AchillesEntityInterceptor<Long> interceptor;

	@Mock
	private Mutator<Object> mutator;

	@Before
	public void setUp()
	{
		when(wideMapMeta.getExternalTableName()).thenReturn("external_cf");
		when((Class<Long>) wideMapMeta.getIdClass()).thenReturn(Long.class);
		when(wideMapMeta.getKeyClass()).thenReturn(Integer.class);
		when(thriftCompositeFactory.createBaseComposite(wideMapMeta, 12)).thenReturn(comp);
		when(context.getWideRowMutator("external_cf")).thenReturn(mutator);

	}

	@Test
	public void should_get_value() throws Exception
	{
		Composite comp = new Composite();
		when(thriftCompositeFactory.createBaseComposite(wideMapMeta, 12)).thenReturn(comp);
		when(dao.getValue(id, comp)).thenReturn("test");
		when(wideMapMeta.castValue("test")).thenReturn("test");

		Object expected = wrapper.get(12);

		assertThat(expected).isEqualTo("test");
	}

	@Test
	public void should_get_null_value() throws Exception
	{
		Composite comp = new Composite();
		when(thriftCompositeFactory.createBaseComposite(wideMapMeta, 12)).thenReturn(comp);
		when(dao.getValue(id, comp)).thenReturn(null);

		assertThat(wrapper.get(12)).isNull();
	}

	@Test
	public void should_insert_value() throws Exception
	{
		when(wideMapMeta.writeValueAsSupportedTypeOrString("test")).thenReturn("test");
		wrapper.insert(12, "test");
		verify(dao).setValueBatch(id, comp, "test", mutator);
		verify(context).flush();
	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		when(wideMapMeta.writeValueAsSupportedTypeOrString("test")).thenReturn("test");
		wrapper.insert(12, "test", 452);
		verify(dao).setValueBatch(id, comp, "test", 452, mutator);
		verify(context).flush();
	}

	@Test
	public void should_find_keyvalues_range() throws Exception
	{
		List<HColumn<Composite, Object>> hColumns = mock(List.class);
		List<KeyValue<Integer, String>> keyValues = mock(List.class);
		Composite startComp = new Composite();
		Composite endComp = new Composite();

		when(
				thriftCompositeFactory.createForQuery(wideMapMeta, 12, 15,
						BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING)) //
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});

		when(dao.findRawColumnsRange(id, startComp, endComp, 10, false)).thenReturn(hColumns);
		when(thriftKeyValueFactory.createKeyValueList(context, wideMapMeta, (List) hColumns))
				.thenReturn(keyValues)
				.thenReturn(keyValues);

		List<KeyValue<Integer, String>> expected = wrapper.find(12, 15, 10);
		assertThat(expected).isSameAs(keyValues);
	}

	@Test
	public void should_find_values_range() throws Exception
	{
		List<HColumn<Composite, Object>> hColumns = mock(List.class);
		List<String> keyValues = mock(List.class);
		Composite startComp = new Composite();
		Composite endComp = new Composite();

		when(
				thriftCompositeFactory.createForQuery(wideMapMeta, 12, 15,
						BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING)) //
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});

		when(dao.findRawColumnsRange(id, startComp, endComp, 10, false)).thenReturn(hColumns);
		when(thriftKeyValueFactory.createValueList(wideMapMeta, (List) hColumns)).thenReturn(
				keyValues).thenReturn(keyValues);

		List<String> expected = wrapper.findValues(12, 15, 10);
		assertThat(expected).isSameAs(keyValues);
	}

	@Test
	public void should_find_keys_range() throws Exception
	{
		List<HColumn<Composite, Object>> hColumns = mock(List.class);
		List<Integer> keyValues = mock(List.class);
		Composite startComp = new Composite();
		Composite endComp = new Composite();

		when(
				thriftCompositeFactory.createForQuery(wideMapMeta, 12, 15,
						BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING)) //
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});

		when(dao.findRawColumnsRange(id, startComp, endComp, 10, false)).thenReturn(hColumns);
		when(thriftKeyValueFactory.createKeyList(wideMapMeta, (List) hColumns)).thenReturn(
				keyValues).thenReturn(keyValues);

		List<Integer> expected = wrapper.findKeys(12, 15, 10);
		assertThat(expected).isSameAs(keyValues);
	}

	@Test
	public void should_get_iterator() throws Exception
	{
		ThriftKeyValueIteratorImpl<Integer, String> keyValues = mock(ThriftKeyValueIteratorImpl.class);
		ThriftSliceIterator<Long, String> iterator = mock(ThriftSliceIterator.class);
		Composite startComp = new Composite();
		Composite endComp = new Composite();

		when(
				thriftCompositeFactory.createForQuery(wideMapMeta, 12, 15,
						BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.ASCENDING)) //
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});
		when(dao.getColumnsIterator(id, startComp, endComp, false, 10)).thenReturn(
				(ThriftSliceIterator) iterator);
		when(thriftIteratorFactory.createKeyValueIterator(context, iterator, wideMapMeta))
				.thenReturn(keyValues);
		KeyValueIterator<Integer, String> expected = wrapper.iterator(12, 15, 10,
				BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.ASCENDING);

		assertThat(expected).isSameAs(keyValues);
	}

	@Test
	public void should_remove() throws Exception
	{
		Composite comp = new Composite();
		when(thriftCompositeFactory.createBaseComposite(wideMapMeta, 12)).thenReturn(comp);

		wrapper.remove(12);

		verify(dao).removeColumnBatch(id, comp, mutator);
		verify(context).flush();
	}

	@Test
	public void should_remove_range() throws Exception
	{
		Composite startComp = new Composite();
		Composite endComp = new Composite();

		when(
				thriftCompositeFactory.createForQuery(wideMapMeta, 12, 15,
						BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.ASCENDING)) //
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});

		wrapper.remove(12, 15, BoundingMode.INCLUSIVE_END_BOUND_ONLY);

		verify(dao).removeColumnRangeBatch(id, startComp, endComp, mutator);
		verify(context).flush();
	}

	@Test
	public void should_remove_first() throws Exception
	{
		wrapper.removeFirst(3);

		verify(dao).removeColumnRangeBatch(id, null, null, false, 3, mutator);
		verify(context).flush();
	}

	@Test
	public void should_remove_last() throws Exception
	{
		wrapper.removeLast(7);

		verify(dao).removeColumnRangeBatch(id, null, null, true, 7, mutator);
		verify(context).flush();
	}
}
