package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.entity.type.WideMap.BoundingMode.INCLUSIVE_BOUNDS;
import static info.archinnov.achilles.entity.type.WideMap.OrderingMode.DESCENDING;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.CounterKeyValueIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * CounterWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class CounterWideMapWrapperTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private CounterWideMapWrapper<Long, Integer> wrapper;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@Mock
	private CounterDao counterDao;

	@Mock
	private PropertyMeta<Integer, Long> propertyMeta;

	@Mock
	private CompositeHelper compositeHelper;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	private IteratorFactory iteratorFactory;

	@Mock
	private CompositeKeyFactory compositeKeyFactory;

	@Mock
	private DynamicCompositeKeyFactory dynamicCompositeKeyFactory;

	@Mock
	private AchillesInterceptor interceptor;

	@Mock
	private Mutator<Composite> counterMutator;

	@Mock
	private AchillesSliceIterator<Composite, DynamicComposite, Long> achillesSliceIterator;

	private Long id = 12L;
	private Integer key = 11;
	private String fqcn = "fqcn";

	private Composite keyComp = new Composite();
	private DynamicComposite comp = new DynamicComposite();

	@Before
	public void setUp()
	{
		wrapper.setId(id);
		wrapper.setFqcn(fqcn);
		when(compositeKeyFactory.createKeyForCounter(fqcn, id, idMeta)).thenReturn(keyComp);

	}

	@Test
	public void should_get_key() throws Exception
	{
		when(dynamicCompositeKeyFactory.createForQuery(propertyMeta, key, EQUAL)).thenReturn(comp);

		when(counterDao.getCounterValue(keyComp, comp)).thenReturn(150L);

		Long actual = wrapper.get(key);

		assertThat(actual).isEqualTo(150L);
	}

	@Test
	public void should_insert() throws Exception
	{
		when(dynamicCompositeKeyFactory.createForInsert(propertyMeta, key)).thenReturn(comp);
		when(interceptor.isBatchMode()).thenReturn(false);

		wrapper.insert(key, 150L);

		verify(counterDao).insertCounter(keyComp, comp, 150L);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_insert_batch() throws Exception
	{
		when(dynamicCompositeKeyFactory.createForInsert(propertyMeta, key)).thenReturn(comp);
		when(interceptor.isBatchMode()).thenReturn(true);
		when((Mutator<Composite>) interceptor.getMutator()).thenReturn(counterMutator);
		wrapper.insert(key, 150L);

		verify(counterDao).insertCounter(keyComp, comp, 150L, counterMutator);
	}

	@Test
	public void should_exception_when_insert_with_ttl() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("Cannot insert counter value with ttl");
		wrapper.insert(key, 150L, 3600);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find() throws Exception
	{
		DynamicComposite start = new DynamicComposite(), end = new DynamicComposite();
		when(
				dynamicCompositeKeyFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS,
						DESCENDING)).thenReturn(new DynamicComposite[]
		{
				start,
				end
		});
		List<HColumn<DynamicComposite, Long>> hColumns = mock(List.class);

		when(counterDao.findRawColumnsRange(keyComp, start, end, 100, DESCENDING.isReverse()))
				.thenReturn(hColumns);
		List<KeyValue<Integer, Long>> expected = new ArrayList<KeyValue<Integer, Long>>();

		when(keyValueFactory.createCounterKeyValueListForDynamicComposite(propertyMeta, hColumns))
				.thenReturn(expected);

		List<KeyValue<Integer, Long>> actual = wrapper.find(11, 12, 100, INCLUSIVE_BOUNDS,
				DESCENDING);

		assertThat(actual).isSameAs(expected);
		verify(compositeHelper).checkBounds(propertyMeta, 11, 12, DESCENDING);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_values() throws Exception
	{
		DynamicComposite start = new DynamicComposite(), end = new DynamicComposite();
		when(
				dynamicCompositeKeyFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS,
						DESCENDING)).thenReturn(new DynamicComposite[]
		{
				start,
				end
		});
		List<HColumn<DynamicComposite, Long>> hColumns = mock(List.class);

		when(counterDao.findRawColumnsRange(keyComp, start, end, 100, DESCENDING.isReverse()))
				.thenReturn(hColumns);

		List<Long> expected = new ArrayList<Long>();

		when(keyValueFactory.createCounterValueListForDynamicComposite(propertyMeta, hColumns))
				.thenReturn(expected);

		List<Long> actual = wrapper.findValues(11, 12, 100, INCLUSIVE_BOUNDS, DESCENDING);

		assertThat(actual).isSameAs(expected);
		verify(compositeHelper).checkBounds(propertyMeta, 11, 12, DESCENDING);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_keys() throws Exception
	{
		DynamicComposite start = new DynamicComposite(), end = new DynamicComposite();
		when(
				dynamicCompositeKeyFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS,
						DESCENDING)).thenReturn(new DynamicComposite[]
		{
				start,
				end
		});
		List<HColumn<DynamicComposite, Long>> hColumns = mock(List.class);

		when(counterDao.findRawColumnsRange(keyComp, start, end, 100, DESCENDING.isReverse()))
				.thenReturn(hColumns);

		List<Integer> expected = new ArrayList<Integer>();

		when(keyValueFactory.createCounterKeyListForDynamicComposite(propertyMeta, hColumns))
				.thenReturn(expected);

		List<Integer> actual = wrapper.findKeys(11, 12, 100, INCLUSIVE_BOUNDS, DESCENDING);

		assertThat(actual).isSameAs(expected);
		verify(compositeHelper).checkBounds(propertyMeta, 11, 12, DESCENDING);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_iterator() throws Exception
	{
		DynamicComposite start = new DynamicComposite(), end = new DynamicComposite();
		when(
				dynamicCompositeKeyFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS,
						DESCENDING)).thenReturn(new DynamicComposite[]
		{
				start,
				end
		});

		when(counterDao.getColumnsIterator(keyComp, start, end, DESCENDING.isReverse(), 100))
				.thenReturn(achillesSliceIterator);
		CounterKeyValueIterator<Integer> expected = mock(CounterKeyValueIterator.class);

		when(
				iteratorFactory.createCounterKeyValueIteratorForDynamicComposite(
						achillesSliceIterator, propertyMeta)).thenReturn(expected);

		KeyValueIterator<Integer, Long> actual = wrapper.iterator(11, 12, 100, INCLUSIVE_BOUNDS,
				DESCENDING);

		assertThat(actual).isSameAs(expected);
	}

	@Test
	public void should_exception_when_remove() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception
				.expectMessage("Cannot remove counter value. Please set a its value to 0 instead of removing it");
		wrapper.remove(11);
	}

	@Test
	public void should_exception_when_remove_range() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception
				.expectMessage("Cannot remove counter value. Please set a its value to 0 instead of removing it");

		wrapper.remove(11, 12, INCLUSIVE_BOUNDS);
	}

	@Test
	public void should_exception_when_remove_first() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception
				.expectMessage("Cannot remove counter value. Please set a its value to 0 instead of removing it");

		wrapper.removeFirst(15);
	}

	@Test
	public void should_exception_when_remove_last() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception
				.expectMessage("Cannot remove counter value. Please set a its value to 0 instead of removing it");

		wrapper.removeLast(15);
	}
}
