package info.archinnov.achilles.proxy.wrapper;

import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_BOUNDS;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static info.archinnov.achilles.type.OrderingMode.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.compound.ThriftCompoundKeyValidator;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.ThriftCounterKeyValueIteratorImpl;
import info.archinnov.achilles.iterator.ThriftCounterSliceIterator;
import info.archinnov.achilles.iterator.factory.ThriftIteratorFactory;
import info.archinnov.achilles.iterator.factory.ThriftKeyValueFactory;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;
import info.archinnov.achilles.type.OrderingMode;
import java.util.ArrayList;
import java.util.List;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * ThriftCounterWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftCounterWideMapWrapperTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private ThriftCounterWideMapWrapper<Integer> wrapper;

    @Mock
    private PropertyMeta<Void, Long> idMeta;

    @Mock
    private ThriftGenericWideRowDao wideMapCounterDao;

    @Mock
    private PropertyMeta<Integer, Counter> propertyMeta;

    @Mock
    private ThriftCompoundKeyValidator compoundKeyValidator;

    @Mock
    private ThriftKeyValueFactory thriftKeyValueFactory;

    @Mock
    private ThriftIteratorFactory thriftIteratorFactory;

    @Mock
    private ThriftCompositeFactory thriftCompositeFactory;

    @Mock
    private ThriftCounterSliceIterator<Long> thriftCounterSliceIterator;

    @Mock
    private ThriftPersistenceContext context;

    @Captor
    private ArgumentCaptor<SafeExecutionContext<Long>> execCaptor;

    @Captor
    private ArgumentCaptor<SafeExecutionContext<Void>> voidExecCaptor;

    @Captor
    private ArgumentCaptor<SafeExecutionContext<List<HCounterColumn<Composite>>>> hColsExecCaptor;

    @Captor
    private ArgumentCaptor<SafeExecutionContext<ThriftCounterSliceIterator<Long>>> iterExecCaptor;

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
        when(thriftCompositeFactory.createKeyForCounter(fqcn, id, idMeta)).thenReturn(keyComp);
        when(propertyMeta.getReadConsistencyLevel()).thenReturn(readLevel);
        when(propertyMeta.getWriteConsistencyLevel()).thenReturn(writeLevel);
    }

    @Test
    public void should_get_counter() throws Exception
    {
        when(thriftCompositeFactory.createForQuery(propertyMeta, key, EQUAL)).thenReturn(comp);
        when(context.executeWithReadConsistencyLevel(execCaptor.capture(), eq(readLevel)))
                .thenReturn(10L);

        Counter actual = wrapper.get(key);

        execCaptor.getValue().execute();

        verify(wideMapCounterDao).getCounterValue(id, comp);

        assertThat(Whitebox.getInternalState(actual, "columnName")).isSameAs(comp);
        assertThat(Whitebox.getInternalState(actual, "counterDao")).isSameAs(wideMapCounterDao);
        assertThat(Whitebox.getInternalState(actual, "context")).isSameAs(context);
        assertThat(Whitebox.getInternalState(actual, "readLevel")).isSameAs(readLevel);
        assertThat(Whitebox.getInternalState(actual, "writeLevel")).isSameAs(writeLevel);
    }

    @Test
    public void should_return_null_when_no_counter_found() throws Exception
    {
        when(thriftCompositeFactory.createForQuery(propertyMeta, key, EQUAL)).thenReturn(comp);
        when(context.executeWithReadConsistencyLevel(execCaptor.capture(), eq(readLevel)))
                .thenReturn(null);

        Counter actual = wrapper.get(key);
        assertThat(actual).isNull();
    }

    @Test
    public void should_insert() throws Exception
    {
        when(thriftCompositeFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);

        wrapper.insert(key, CounterBuilder.incr(150L));
        verify(context).executeWithWriteConsistencyLevel(voidExecCaptor.capture(), eq(writeLevel));

        voidExecCaptor.getValue().execute();

        verify(wideMapCounterDao).incrementCounter(id, comp, 150L);
    }

    @Test
    public void should_exception_when_insert_with_ttl() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot insert counter value with ttl");
        wrapper.insert(key, CounterBuilder.incr(150L), 3600);
    }

    @Test
    public void should_find() throws Exception
    {
        Composite start = new Composite(), end = new Composite();
        when(
                thriftCompositeFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS,
                        DESCENDING)).thenReturn(new Composite[]
        {
                start,
                end
        });
        List<HCounterColumn<Composite>> hColumns = mock(List.class);

        when(wideMapCounterDao.findCounterColumnsRange(id, start, end, 100, DESCENDING.isReverse()))
                .thenReturn(hColumns);
        List<KeyValue<Integer, Counter>> expected = new ArrayList<KeyValue<Integer, Counter>>();

        when(context.executeWithReadConsistencyLevel(hColsExecCaptor.capture(), eq(readLevel)))
                .thenReturn(hColumns);
        when(thriftKeyValueFactory.createCounterKeyValueList(context, propertyMeta, hColumns))
                .thenReturn(expected);

        List<KeyValue<Integer, Counter>> actual = wrapper.find(11, 12, 100, INCLUSIVE_BOUNDS,
                DESCENDING);
        List<HCounterColumn<Composite>> actual2 = hColsExecCaptor.getValue().execute();

        assertThat(actual).isSameAs(expected);
        assertThat(actual2).isSameAs(hColumns);

        verify(compoundKeyValidator).validateBoundsForQuery(propertyMeta, 11, 12, DESCENDING);
    }

    @Test
    public void should_find_values() throws Exception
    {
        Composite start = new Composite(), end = new Composite();
        when(
                thriftCompositeFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS,
                        DESCENDING)).thenReturn(new Composite[]
        {
                start,
                end
        });
        List<HCounterColumn<Composite>> hColumns = mock(List.class);

        when(wideMapCounterDao.findCounterColumnsRange(id, start, end, 100, DESCENDING.isReverse()))
                .thenReturn(hColumns);

        List<Counter> expected = new ArrayList<Counter>();
        when(context.executeWithReadConsistencyLevel(hColsExecCaptor.capture(), eq(readLevel)))
                .thenReturn(hColumns);
        when(thriftKeyValueFactory.createCounterValueList(context, propertyMeta, hColumns))
                .thenReturn(expected);

        List<Counter> actual = wrapper.findValues(11, 12, 100, INCLUSIVE_BOUNDS, DESCENDING);
        List<HCounterColumn<Composite>> actual2 = hColsExecCaptor.getValue().execute();

        assertThat(actual).isSameAs(expected);
        assertThat(actual2).isSameAs(hColumns);

        verify(compoundKeyValidator).validateBoundsForQuery(propertyMeta, 11, 12, DESCENDING);
    }

    @Test
    public void should_find_keys() throws Exception
    {
        Composite start = new Composite(), end = new Composite();
        when(
                thriftCompositeFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS,
                        DESCENDING)).thenReturn(new Composite[]
        {
                start,
                end
        });
        List<HCounterColumn<Composite>> hColumns = mock(List.class);

        when(wideMapCounterDao.findCounterColumnsRange(id, start, end, 100, DESCENDING.isReverse()))
                .thenReturn(hColumns);

        List<Integer> expected = new ArrayList<Integer>();
        when(context.executeWithReadConsistencyLevel(hColsExecCaptor.capture(), eq(readLevel)))
                .thenReturn(hColumns);
        when(thriftKeyValueFactory.createCounterKeyList(propertyMeta, hColumns)).thenReturn(
                expected);

        List<Integer> actual = wrapper.findKeys(11, 12, 100, INCLUSIVE_BOUNDS, DESCENDING);
        List<HCounterColumn<Composite>> actual2 = hColsExecCaptor.getValue().execute();

        assertThat(actual).isSameAs(expected);
        assertThat(actual2).isSameAs(hColumns);

        verify(compoundKeyValidator).validateBoundsForQuery(propertyMeta, 11, 12, DESCENDING);
    }

    @Test
    public void should_create_iterator() throws Exception
    {
        Composite start = new Composite(), end = new Composite();
        when(
                thriftCompositeFactory.createForQuery(propertyMeta, 11, 12, INCLUSIVE_BOUNDS,
                        DESCENDING)).thenReturn(new Composite[]
        {
                start,
                end
        });

        when(
                wideMapCounterDao.getCounterColumnsIterator(id, start, end, DESCENDING.isReverse(),
                        100)).thenReturn(thriftCounterSliceIterator);
        ThriftCounterKeyValueIteratorImpl<Integer> expected = mock(ThriftCounterKeyValueIteratorImpl.class);

        when(context.executeWithReadConsistencyLevel(iterExecCaptor.capture(), eq(readLevel)))
                .thenReturn(thriftCounterSliceIterator);
        when(
                thriftIteratorFactory.createCounterKeyValueIterator(context,
                        thriftCounterSliceIterator, propertyMeta)).thenReturn(expected);

        KeyValueIterator<Integer, Counter> actual = wrapper.iterator(11, 12, 100, INCLUSIVE_BOUNDS,
                DESCENDING);
        ThriftCounterSliceIterator<Long> actual2 = iterExecCaptor.getValue().execute();

        assertThat(actual).isSameAs(expected);
        assertThat(actual2).isSameAs(thriftCounterSliceIterator);
    }

    @Test
    public void should_exception_when_remove() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");
        wrapper.remove(11);
    }

    @Test
    public void should_exception_when_remove_range() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");

        wrapper.remove(11, 12, INCLUSIVE_BOUNDS);
    }

    @Test
    public void should_exception_when_remove_first() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");

        wrapper.removeFirst(15);
    }

    @Test
    public void should_exception_when_remove_last() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");

        wrapper.removeLast(15);
    }

    @Test
    public void should_not_get_with_consistency_level() throws Exception
    {
        wrapper.get(10, EACH_QUORUM);
    }

    @Test
    public void should_not_insert_with_consistency_level() throws Exception
    {
        when(thriftCompositeFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
        wrapper.insert(10, CounterBuilder.incr());
    }

    @Test
    public void should_exception_when_insert_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");

        wrapper.insert(15, CounterBuilder.incr(), EACH_QUORUM);
    }

    @Test
    public void should_exception_when_insert_with_ttl_and_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot insert counter value with ttl");

        wrapper.insert(15, CounterBuilder.incr(), 10, EACH_QUORUM);
    }

    @Test
    public void should_not_find_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");

        wrapper.find(10, 11, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_find_complete_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");

        wrapper.find(10, 11, 100, INCLUSIVE_BOUNDS, ASCENDING, EACH_QUORUM);
    }

    @Test
    public void should_not_findBoundsExclusive_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findBoundsExclusive(10, 11, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_findReverse_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findReverse(11, 10, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_findReverseBoundsExclusive_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findReverseBoundsExclusive(11, 10, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_findFirst_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findFirst(EACH_QUORUM);
    }

    @Test
    public void should_not_findFirst_n_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findFirst(10, EACH_QUORUM);
    }

    @Test
    public void should_not_findLast_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findLast(EACH_QUORUM);
    }

    @Test
    public void should_not_findLast_n_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findLast(10, EACH_QUORUM);
    }

    @Test
    public void should_not_findValues_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findValues(10, 11, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_findValues_complete_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findValues(10, 11, 100, BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.ASCENDING,
                EACH_QUORUM);
    }

    @Test
    public void should_not_findBoundsExclusiveValues_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findBoundsExclusiveValues(10, 11, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_findReverseValues_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findReverseValues(11, 10, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_findReverseBoundsExclusiveValues_with_consistency_level()
            throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findReverseBoundsExclusiveValues(11, 10, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_findFirstValue_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findFirstValue(EACH_QUORUM);
    }

    @Test
    public void should_not_findFirstValues_n_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findFirstValues(11, EACH_QUORUM);
    }

    @Test
    public void should_not_findLastValue_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findLastValue(EACH_QUORUM);
    }

    @Test
    public void should_not_findLastValue_n_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findLastValues(11, EACH_QUORUM);
    }

    @Test
    public void should_not_findKeys_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findKeys(10, 11, 100, BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.ASCENDING,
                EACH_QUORUM);
    }

    @Test
    public void should_not_findKeys_complete_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findKeys(10, 11, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_findBoundsExclusiveKeys_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findBoundsExclusiveKeys(10, 11, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_findReverseKeys_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findReverseKeys(11, 10, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_findReverseBoundsExclusiveKeys_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findReverseBoundsExclusiveKeys(11, 10, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_findFirstKey_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findFirstKey(EACH_QUORUM);
    }

    @Test
    public void should_not_findFirstKeys_n_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findFirstKeys(10, EACH_QUORUM);
    }

    @Test
    public void should_not_findLastKey_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findLastKey(EACH_QUORUM);
    }

    @Test
    public void should_not_findLastKeys_n_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.findLastKeys(10, EACH_QUORUM);
    }

    @Test
    public void should_not_iterate_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.iterator(EACH_QUORUM);
    }

    @Test
    public void should_not_iterate_complete_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.iterator(10, 11, 100, BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.ASCENDING,
                EACH_QUORUM);
    }

    @Test
    public void should_not_iterate_n_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.iterator(100, EACH_QUORUM);
    }

    @Test
    public void should_not_iterate_simple_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.iterator(10, 11, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_iteratorBoundsExclusive_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.iteratorBoundsExclusive(10, 11, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_iteratorReverse_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.iteratorReverse(EACH_QUORUM);
    }

    @Test
    public void should_not_iteratorReverse_n_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.iteratorReverse(100, EACH_QUORUM);
    }

    @Test
    public void should_not_iteratorReverse_complete_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.iteratorReverse(11, 10, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_iteratorReverseBoundsExclusive_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception
                .expectMessage("Please set runtime consistency level at Counter level instead of at WideMap level");
        wrapper.iteratorReverseBoundsExclusive(11, 10, 100, EACH_QUORUM);
    }

    @Test
    public void should_not_remove_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");

        wrapper.remove(10, EACH_QUORUM);
    }

    @Test
    public void should_not_remove_range_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");

        wrapper.remove(10, 11, EACH_QUORUM);
    }

    @Test
    public void should_not_remove_complete_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");

        wrapper.remove(10, 11, BoundingMode.EXCLUSIVE_BOUNDS, EACH_QUORUM);
    }

    @Test
    public void should_not_removeBoundsExclusive_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");

        wrapper.remove(10, 11, EACH_QUORUM);
    }

    @Test
    public void should_not_removeFirst_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");

        wrapper.removeFirst(EACH_QUORUM);
    }

    @Test
    public void should_not_removeFirst_n_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");

        wrapper.removeFirst(10, EACH_QUORUM);
    }

    @Test
    public void should_not_removeLast_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");

        wrapper.removeLast(EACH_QUORUM);
    }

    @Test
    public void should_not_removeLast_n_with_consistency_level() throws Exception
    {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Cannot remove counter value");

        wrapper.removeLast(10, EACH_QUORUM);
    }
}
