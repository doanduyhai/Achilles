package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.serializer.SerializerUtils.DYNA_COMP_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.CounterSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
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
 * AchillesCounterSliceIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class AchillesCounterSliceIteratorTest
{

	@Mock
	private PropertyMeta<Integer, Long> propertyMeta;

	@Mock
	private SliceCounterQuery<Long, DynamicComposite> query;

	@Mock
	private CounterSlice<DynamicComposite> counterSlice;

	@Mock
	private List<HCounterColumn<DynamicComposite>> hCounterColumns;

	@Mock
	private QueryResult<CounterSlice<DynamicComposite>> queryResult;

	@Mock
	private Iterator<HCounterColumn<DynamicComposite>> counterColumnsIterator;

	private AchillesCounterSliceIterator<Long, DynamicComposite> iterator;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	private String columnFamily = "cf";

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Before
	public void setUp()
	{
		when(query.execute()).thenReturn(queryResult);
		when(queryResult.get()).thenReturn(counterSlice);
		when(counterSlice.getColumns()).thenReturn(hCounterColumns);
		when(hCounterColumns.iterator()).thenReturn(counterColumnsIterator);
		when(propertyMeta.getValueClass()).thenReturn(Long.class);
		when(propertyMeta.getValueSerializer()).thenReturn((Serializer) LONG_SRZ);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_3_values() throws Exception
	{
		DynamicComposite start = new DynamicComposite(), //
		end = new DynamicComposite(), //
		name1 = new DynamicComposite(), //
		name2 = new DynamicComposite(), //
		name3 = new DynamicComposite();

		name1.addComponent("name1", STRING_SRZ);
		name2.addComponent("name2", STRING_SRZ);
		name3.addComponent("name3", STRING_SRZ);

		Long val1 = 11L, val2 = 12L, val3 = 13L;

		HCounterColumn<DynamicComposite> hCol1 = HFactory.createCounterColumn(name1, val1,
				DYNA_COMP_SRZ);
		HCounterColumn<DynamicComposite> hCol2 = HFactory.createCounterColumn(name2, val2,
				DYNA_COMP_SRZ);
		HCounterColumn<DynamicComposite> hCol3 = HFactory.createCounterColumn(name3, val3,
				DYNA_COMP_SRZ);

		when(counterColumnsIterator.hasNext()).thenReturn(true, true, true, true, true, false);
		when(counterColumnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		when(policy.getCurrentReadLevel()).thenReturn(LOCAL_QUORUM, ONE);

		iterator = new AchillesCounterSliceIterator<Long, DynamicComposite>(policy, columnFamily,
				query, start, end, false, 10);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<DynamicComposite> h1 = iterator.next();

		assertThat(h1.getName()).isEqualTo(name1);
		assertThat(h1.getValue()).isEqualTo(val1);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<DynamicComposite> h2 = iterator.next();

		assertThat(h2.getName()).isEqualTo(name2);
		assertThat(h2.getValue()).isEqualTo(val2);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<DynamicComposite> h3 = iterator.next();

		assertThat(h3.getName()).isEqualTo(name3);
		assertThat(h3.getValue()).isEqualTo(val3);

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(policy, atLeastOnce()).setCurrentReadLevel(LOCAL_QUORUM);
		verify(policy, atLeastOnce()).setCurrentReadLevel(ONE);
		verify(policy).loadConsistencyLevelForRead(columnFamily);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_reload_when_reaching_end_of_batch() throws Exception
	{
		DynamicComposite start = new DynamicComposite(), //
		end = new DynamicComposite(), //
		name1 = new DynamicComposite(), //
		name2 = new DynamicComposite(), //
		name3 = new DynamicComposite();
		int count = 2;

		name1.addComponent("name1", STRING_SRZ);
		name2.addComponent("name2", STRING_SRZ);
		name3.addComponent("name3", STRING_SRZ);

		Long val1 = 11L, val2 = 12L, val3 = 13L;

		HCounterColumn<DynamicComposite> hCol1 = HFactory.createCounterColumn(name1, val1,
				DYNA_COMP_SRZ);
		HCounterColumn<DynamicComposite> hCol2 = HFactory.createCounterColumn(name2, val2,
				DYNA_COMP_SRZ);
		HCounterColumn<DynamicComposite> hCol3 = HFactory.createCounterColumn(name3, val3,
				DYNA_COMP_SRZ);

		when(counterColumnsIterator.hasNext()).thenReturn(true, true, true, false, true, false,
				false);
		when(counterColumnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		when(policy.getCurrentReadLevel()).thenReturn(LOCAL_QUORUM, ONE);

		iterator = new AchillesCounterSliceIterator<Long, DynamicComposite>(policy, columnFamily,
				query, start, end, false, count);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<DynamicComposite> h1 = iterator.next();

		assertThat(h1.getName()).isEqualTo(name1);
		assertThat(h1.getValue()).isEqualTo(val1);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<DynamicComposite> h2 = iterator.next();

		assertThat(h2.getName()).isEqualTo(name2);
		assertThat(h2.getValue()).isEqualTo(val2);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HCounterColumn<DynamicComposite> h3 = iterator.next();

		assertThat(h3.getName()).isEqualTo(name3);
		assertThat(h3.getValue()).isEqualTo(val3);

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(query).setRange(name2, end, false, count);

		verify(policy, times(2)).setCurrentReadLevel(LOCAL_QUORUM);
		verify(policy, times(2)).setCurrentReadLevel(ONE);
		verify(policy, times(2)).loadConsistencyLevelForRead(columnFamily);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_remove() throws Exception
	{
		DynamicComposite start = new DynamicComposite(), end = new DynamicComposite();
		iterator = new AchillesCounterSliceIterator<Long, DynamicComposite>(policy, columnFamily,
				query, start, end, false, 10);

		iterator.remove();
	}
}
