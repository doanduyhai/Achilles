package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.CounterSlice;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceCounterQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * ThriftCounterSliceIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftCounterSliceIteratorTest
{

	@Mock
	private PropertyMeta<Integer, Long> propertyMeta;

	@Mock
	private SliceCounterQuery<Long, Composite> query;

	@Mock
	private CounterSlice<Composite> counterSlice;

	@Mock
	private List<HCounterColumn<Composite>> hCounterColumns;

	@Mock
	private QueryResult<CounterSlice<Composite>> queryResult;

	@Mock
	private Iterator<HCounterColumn<Composite>> counterColumnsIterator;

	private ThriftCounterSliceIterator<Long> iterator;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	private String columnFamily = "cf";

	@Before
	public void setUp()
	{
		when(query.execute()).thenReturn(queryResult);
		when(queryResult.get()).thenReturn(counterSlice);
		when(counterSlice.getColumns()).thenReturn(hCounterColumns);
		when(hCounterColumns.iterator()).thenReturn(counterColumnsIterator);
		when(propertyMeta.getValueClass()).thenReturn(Long.class);

	}

	@Test
	public void should_return_3_values() throws Exception
	{
		Composite start = new Composite(), //
		end = new Composite(), //
		name1 = new Composite(), //
		name2 = new Composite(), //
		name3 = new Composite();

		name1.addComponent("name1", STRING_SRZ);
		name2.addComponent("name2", STRING_SRZ);
		name3.addComponent("name3", STRING_SRZ);

		Long val1 = 11L, val2 = 12L, val3 = 13L;

		HCounterColumn<Composite> hCol1 = HFactory.createCounterColumn(name1, val1, COMPOSITE_SRZ);
		HCounterColumn<Composite> hCol2 = HFactory.createCounterColumn(name2, val2, COMPOSITE_SRZ);
		HCounterColumn<Composite> hCol3 = HFactory.createCounterColumn(name3, val3, COMPOSITE_SRZ);

		when(counterColumnsIterator.hasNext()).thenReturn(true, true, true, true, true, false);
		when(counterColumnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		when(policy.getCurrentReadLevel()).thenReturn(LOCAL_QUORUM, ONE);

		iterator = new ThriftCounterSliceIterator<Long>(policy, columnFamily, query, start, end,
				false, 10);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<Composite> h1 = iterator.next();

		assertThat(h1.getName().get(0, STRING_SRZ)).isEqualTo("name1");
		assertThat(h1.getValue()).isEqualTo(val1);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<Composite> h2 = iterator.next();

		assertThat(h2.getName().get(0, STRING_SRZ)).isEqualTo("name2");
		assertThat(h2.getValue()).isEqualTo(val2);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<Composite> h3 = iterator.next();

		assertThat(h3.getName().get(0, STRING_SRZ)).isEqualTo("name3");
		assertThat(h3.getValue()).isEqualTo(val3);

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(policy, atLeastOnce()).setCurrentReadLevel(LOCAL_QUORUM);
		verify(policy, atLeastOnce()).setCurrentReadLevel(ONE);
		verify(policy).loadConsistencyLevelForRead(columnFamily);
	}

	@Test
	public void should_reload_when_reaching_end_of_batch() throws Exception
	{
		Composite start = new Composite(), //
		end = new Composite(), //
		name1 = new Composite(), //
		name2 = new Composite(), //
		name3 = new Composite();
		int count = 2;

		name1.addComponent("name1", STRING_SRZ);
		name2.addComponent("name2", STRING_SRZ);
		name3.addComponent("name3", STRING_SRZ);

		Long val1 = 11L, val2 = 12L, val3 = 13L;

		HCounterColumn<Composite> hCol1 = HFactory.createCounterColumn(name1, val1, COMPOSITE_SRZ);
		HCounterColumn<Composite> hCol2 = HFactory.createCounterColumn(name2, val2, COMPOSITE_SRZ);
		HCounterColumn<Composite> hCol3 = HFactory.createCounterColumn(name3, val3, COMPOSITE_SRZ);

		when(counterColumnsIterator.hasNext()).thenReturn(true, true, true, false, true, false,
				false);
		when(counterColumnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		when(policy.getCurrentReadLevel()).thenReturn(LOCAL_QUORUM, ONE);

		iterator = new ThriftCounterSliceIterator<Long>(policy, columnFamily, query, start, end,
				false, count);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<Composite> h1 = iterator.next();

		assertThat(h1.getName().get(0, STRING_SRZ)).isEqualTo("name1");
		assertThat(h1.getValue()).isEqualTo(val1);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<Composite> h2 = iterator.next();

		assertThat(h2.getName().get(0, STRING_SRZ)).isEqualTo("name2");
		assertThat(h2.getValue()).isEqualTo(val2);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<Composite> h3 = iterator.next();

		assertThat(h3.getName().get(0, STRING_SRZ)).isEqualTo("name3");
		assertThat(h3.getValue()).isEqualTo(val3);

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(policy, times(2)).setCurrentReadLevel(LOCAL_QUORUM);
		verify(policy, times(2)).setCurrentReadLevel(ONE);
		verify(policy, times(2)).loadConsistencyLevelForRead(columnFamily);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_remove() throws Exception
	{
		Composite start = new Composite(), end = new Composite();
		iterator = new ThriftCounterSliceIterator<Long>(policy, columnFamily, query, start, end,
				false, 10);

		iterator.remove();
	}
}
