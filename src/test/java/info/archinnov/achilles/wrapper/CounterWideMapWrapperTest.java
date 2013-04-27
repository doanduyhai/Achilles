package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.*;
import static info.archinnov.achilles.entity.type.WideMap.BoundingMode.INCLUSIVE_BOUNDS;
import static info.archinnov.achilles.entity.type.WideMap.OrderingMode.DESCENDING;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Counter;
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

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
	private PropertyMeta<Integer, Counter> propertyMeta;

	@Mock
	private CompositeHelper compositeHelper;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	private IteratorFactory iteratorFactory;

	@Mock
	private CompositeFactory compositeFactory;

	@Mock
	private AchillesCounterSliceIterator<Long> achillesCounterSliceIterator;

	@Mock
	private PersistenceContext<Long> context;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	private Long id = 12L;
	private Integer key = 11;
	private String fqcn = "fqcn";

	private Composite keyComp = new Composite();
	private Composite comp = new Composite();
	private ConsistencyLevel readLevel = EACH_QUORUM;
	private ConsistencyLevel writeLevel = LOCAL_QUORUM;

	@Before
	public void setUp()
	{
		wrapper.setId(id);
		when(compositeFactory.createKeyForCounter(fqcn, id, idMeta)).thenReturn(keyComp);
		when(context.getPolicy()).thenReturn(policy);
		when(propertyMeta.getReadConsistencyLevel()).thenReturn(readLevel);
		when(propertyMeta.getWriteConsistencyLevel()).thenReturn(writeLevel);
	}

	@Test
	public void should_get_key() throws Exception
	{
		when(compositeFactory.createForQuery(propertyMeta, key, EQUAL)).thenReturn(comp);
		Counter actual = wrapper.get(key);

		assertThat(Whitebox.getInternalState(actual, "columnName")).isSameAs(comp);
		assertThat(Whitebox.getInternalState(actual, "counterDao")).isSameAs(wideMapCounterDao);
		assertThat(Whitebox.getInternalState(actual, "policy")).isSameAs(policy);
		assertThat(Whitebox.getInternalState(actual, "readLevel")).isSameAs(readLevel);
		assertThat(Whitebox.getInternalState(actual, "writeLevel")).isSameAs(writeLevel);

	}

	@Test
	public void should_insert() throws Exception
	{
		when(compositeFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);

		wrapper.insert(key, CounterBuilder.incr(150L));

		verify(wideMapCounterDao).incrementCounter(id, comp, 150L);
		verify(context).flush();
	}

	@Test
	public void should_exception_when_insert_with_ttl() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("Cannot insert counter value with ttl");
		wrapper.insert(key, CounterBuilder.incr(150L), 3600);
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
		List<KeyValue<Integer, Counter>> expected = new ArrayList<KeyValue<Integer, Counter>>();

		when(keyValueFactory.createCounterKeyValueList(context, propertyMeta, hColumns))
				.thenReturn(expected);

		List<KeyValue<Integer, Counter>> actual = wrapper.find(11, 12, 100, INCLUSIVE_BOUNDS,
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

		List<Counter> expected = new ArrayList<Counter>();

		when(keyValueFactory.createCounterValueList(context, propertyMeta, hColumns)).thenReturn(
				expected);

		List<Counter> actual = wrapper.findValues(11, 12, 100, INCLUSIVE_BOUNDS, DESCENDING);

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
		CounterKeyValueIteratorImpl<Long, Integer> expected = mock(CounterKeyValueIteratorImpl.class);

		when(
				iteratorFactory.createCounterKeyValueIterator(context,
						achillesCounterSliceIterator, propertyMeta)).thenReturn(expected);

		KeyValueIterator<Integer, Counter> actual = wrapper.iterator(11, 12, 100, INCLUSIVE_BOUNDS,
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
