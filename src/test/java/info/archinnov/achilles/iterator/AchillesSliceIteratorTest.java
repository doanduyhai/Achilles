package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.*;
import static info.archinnov.achilles.serializer.SerializerUtils.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.util.Iterator;
import java.util.List;

import mapping.entity.UserBean;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.HColumnTestBuilder;

/**
 * AchillesSliceIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class AchillesSliceIteratorTest
{

	@Mock
	private PropertyMeta<Integer, UserBean> propertyMeta;

	@Mock
	private SliceQuery<Long, Composite, String> query;

	@Mock
	private QueryResult<ColumnSlice<Composite, String>> queryResult;

	@Mock
	private ColumnSlice<Composite, String> columnSlice;

	@Mock
	private List<HColumn<Composite, String>> hColumns;

	@Mock
	private Iterator<HColumn<Composite, String>> columnsIterator;

	private AchillesSliceIterator<Long, String> iterator;

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
		when(queryResult.get()).thenReturn(columnSlice);
		when(columnSlice.getColumns()).thenReturn(hColumns);
		when(hColumns.iterator()).thenReturn(columnsIterator);
		when(propertyMeta.getValueClass()).thenReturn(UserBean.class);
		when(propertyMeta.getValueSerializer()).thenReturn((Serializer) OBJECT_SRZ);

	}

	@SuppressWarnings("unchecked")
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

		String val1 = "val1", val2 = "val2", val3 = "val3";
		int ttl = 10;

		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(name1, val1, ttl);
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(name2, val2, ttl);
		HColumn<Composite, String> hCol3 = HColumnTestBuilder.simple(name3, val3, ttl);

		when(columnsIterator.hasNext()).thenReturn(true, true, true, true, true, false);
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		when(policy.getCurrentReadLevel()).thenReturn(LOCAL_QUORUM, ONE);

		iterator = new AchillesSliceIterator<Long, String>(policy, columnFamily, query, start, end,
				false, 10);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h1 = iterator.next();

		assertThat(h1.getNameBytes()).isEqualTo(name1.serialize());
		assertThat(h1.getValue()).isEqualTo(val1);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h2 = iterator.next();

		assertThat(h2.getNameBytes()).isEqualTo(name2.serialize());
		assertThat(h2.getValue()).isEqualTo(val2);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h3 = iterator.next();

		assertThat(h3.getNameBytes()).isEqualTo(name3.serialize());
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
		Composite start = new Composite(), //
		end = new Composite(), //
		name1 = new Composite(), //
		name2 = new Composite(), //
		name3 = new Composite();
		int count = 2;

		name1.addComponent("name1", STRING_SRZ);
		name2.addComponent("name2", STRING_SRZ);
		name3.addComponent("name3", STRING_SRZ);

		String val1 = "val1", val2 = "val2", val3 = "val3";
		int ttl = 10;

		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(name1, val1, ttl);
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(name2, val2, ttl);
		HColumn<Composite, String> hCol3 = HColumnTestBuilder.simple(name3, val3, ttl);

		when(columnsIterator.hasNext()).thenReturn(true, true, true, false, true, false, false);
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		iterator = new AchillesSliceIterator<Long, String>(policy, columnFamily, query, start, end,
				false, count);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h1 = iterator.next();

		assertThat(h1.getNameBytes()).isEqualTo(name1.serialize());
		assertThat(h1.getValue()).isEqualTo(val1);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h2 = iterator.next();

		assertThat(h2.getNameBytes()).isEqualTo(name2.serialize());
		assertThat(h2.getValue()).isEqualTo(val2);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h3 = iterator.next();

		assertThat(h3.getNameBytes()).isEqualTo(name3.serialize());
		assertThat(h3.getValue()).isEqualTo(val3);

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(policy).getCurrentReadLevel();
		verify(policy, never()).setCurrentReadLevel(any(ConsistencyLevel.class));

	}
}
