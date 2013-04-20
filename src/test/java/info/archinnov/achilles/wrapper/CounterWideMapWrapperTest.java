package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.entity.type.WideMap.BoundingMode.INCLUSIVE_BOUNDS;
import static info.archinnov.achilles.entity.type.WideMap.OrderingMode.DESCENDING;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesCounterSliceIterator;
import info.archinnov.achilles.iterator.CounterKeyValueIteratorImpl;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;
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
	private GenericColumnFamilyDao<Long, Long> wideMapCounterDao;

	@Mock
	private PropertyMeta<Integer, Long> propertyMeta;

	@Mock
	private CompositeHelper compositeHelper;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	private IteratorFactory iteratorFactory;

	@Mock
	private CompositeFactory compositeFactory;

	@Mock
	private Mutator<Long> counterMutator;

	@Mock
	private AchillesCounterSliceIterator<Long> achillesCounterSliceIterator;

	@Mock
	private PersistenceContext<Long> context;

	private Long id = 12L;
	private Integer key = 11;
	private String fqcn = "fqcn";

	private Composite keyComp = new Composite();
	private Composite comp = new Composite();

	@Before
	public void setUp()
	{
		wrapper.setContext(context);
		wrapper.setId(id);
		wrapper.setFqcn(fqcn);
		when(compositeFactory.createKeyForCounter(fqcn, id, idMeta)).thenReturn(keyComp);

	}

	@Test
	public void should_get_key() throws Exception
	{
		when(compositeFactory.createForQuery(propertyMeta, key, EQUAL)).thenReturn(comp);

		when(wideMapCounterDao.getCounterValue(id, comp)).thenReturn(150L);

		Long actual = wrapper.get(key);

		assertThat(actual).isEqualTo(150L);
	}

	@Test
	public void should_insert() throws Exception
	{
		when(propertyMeta.getExternalCFName()).thenReturn("external_cf");
		when(compositeFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
		when(context.getColumnFamilyMutator("external_cf")).thenReturn(counterMutator);
		wrapper.insert(key, 150L);

		verify(wideMapCounterDao).insertCounterBatch(id, comp, 150L, counterMutator);
		verify(context).flush();
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
		Composite start = new Composite(), end = new Composite();
		when(compositeFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS, DESCENDING))
				.thenReturn(new Composite[]
				{
						start,
						end
				});
		List<HCounterColumn<Composite>> hColumns = mock(List.class);

		when(wideMapCounterDao.findCounterColumnsRange(id, start, end, 100, DESCENDING.isReverse()))
				.thenReturn(hColumns);
		List<KeyValue<Integer, Long>> expected = new ArrayList<KeyValue<Integer, Long>>();

		when(keyValueFactory.createCounterKeyValueList(propertyMeta, hColumns))
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
		Composite start = new Composite(), end = new Composite();
		when(compositeFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS, DESCENDING))
				.thenReturn(new Composite[]
				{
						start,
						end
				});
		List<HCounterColumn<Composite>> hColumns = mock(List.class);

		when(wideMapCounterDao.findCounterColumnsRange(id, start, end, 100, DESCENDING.isReverse()))
				.thenReturn(hColumns);

		List<Long> expected = new ArrayList<Long>();

		when(keyValueFactory.createCounterValueList(propertyMeta, hColumns)).thenReturn(expected);

		List<Long> actual = wrapper.findValues(11, 12, 100, INCLUSIVE_BOUNDS, DESCENDING);

		assertThat(actual).isSameAs(expected);
		verify(compositeHelper).checkBounds(propertyMeta, 11, 12, DESCENDING);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_keys() throws Exception
	{
		Composite start = new Composite(), end = new Composite();
		when(compositeFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS, DESCENDING))
				.thenReturn(new Composite[]
				{
						start,
						end
				});
		List<HCounterColumn<Composite>> hColumns = mock(List.class);

		when(wideMapCounterDao.findCounterColumnsRange(id, start, end, 100, DESCENDING.isReverse()))
				.thenReturn(hColumns);

		List<Integer> expected = new ArrayList<Integer>();

		when(keyValueFactory.createCounterKeyList(propertyMeta, hColumns)).thenReturn(expected);

		List<Integer> actual = wrapper.findKeys(11, 12, 100, INCLUSIVE_BOUNDS, DESCENDING);

		assertThat(actual).isSameAs(expected);
		verify(compositeHelper).checkBounds(propertyMeta, 11, 12, DESCENDING);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_iterator() throws Exception
	{
		Composite start = new Composite(), end = new Composite();
		when(compositeFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS, DESCENDING))
				.thenReturn(new Composite[]
				{
						start,
						end
				});

		when(
				wideMapCounterDao.getCounterColumnsIterator(id, start, end, DESCENDING.isReverse(),
						100)).thenReturn(achillesCounterSliceIterator);
		CounterKeyValueIteratorImpl<Integer> expected = mock(CounterKeyValueIteratorImpl.class);

		when(
				iteratorFactory.createCounterKeyValueIterator(achillesCounterSliceIterator,
						propertyMeta)).thenReturn(expected);

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
