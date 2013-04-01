package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.serializer.SerializerUtils.DYNA_COMP_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.OBJECT_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.Iterator;
import java.util.List;

import mapping.entity.UserBean;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
	private SliceQuery<Long, DynamicComposite, String> query;

	@Mock
	private QueryResult<ColumnSlice<DynamicComposite, String>> queryResult;

	@Mock
	private ColumnSlice<DynamicComposite, String> columnSlice;

	@Mock
	private List<HColumn<DynamicComposite, String>> hColumns;

	@Mock
	private Iterator<HColumn<DynamicComposite, String>> columnsIterator;

	private AchillesSliceIterator<Long, DynamicComposite, String> iterator;

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
		DynamicComposite start = new DynamicComposite(), //
		end = new DynamicComposite(), //
		name1 = new DynamicComposite(), //
		name2 = new DynamicComposite(), //
		name3 = new DynamicComposite();

		name1.addComponent("name1", STRING_SRZ);
		name2.addComponent("name2", STRING_SRZ);
		name3.addComponent("name3", STRING_SRZ);

		String val1 = "val1", val2 = "val2", val3 = "val3";
		long ttl = 10L;

		HColumn<DynamicComposite, String> hCol1 = HFactory.createColumn(name1, val1, ttl,
				DYNA_COMP_SRZ, STRING_SRZ);
		HColumn<DynamicComposite, String> hCol2 = HFactory.createColumn(name2, val2, ttl,
				DYNA_COMP_SRZ, STRING_SRZ);
		HColumn<DynamicComposite, String> hCol3 = HFactory.createColumn(name3, val3, ttl,
				DYNA_COMP_SRZ, STRING_SRZ);

		when(columnsIterator.hasNext()).thenReturn(true, true, true, true, true, false);
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		iterator = new AchillesSliceIterator<Long, DynamicComposite, String>(policy, columnFamily,
				query, start, end, false, 10);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, String> h1 = iterator.next();

		assertThat(h1.getName()).isEqualTo(name1);
		assertThat(h1.getValue()).isEqualTo(val1);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, String> h2 = iterator.next();

		assertThat(h2.getName()).isEqualTo(name2);
		assertThat(h2.getValue()).isEqualTo(val2);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, String> h3 = iterator.next();

		assertThat(h3.getName()).isEqualTo(name3);
		assertThat(h3.getValue()).isEqualTo(val3);

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(policy).loadConsistencyLevelForRead(columnFamily);
		verify(policy).reinitDefaultConsistencyLevel();

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

		String val1 = "val1", val2 = "val2", val3 = "val3";
		long ttl = 10L;

		HColumn<DynamicComposite, String> hCol1 = HFactory.createColumn(name1, val1, ttl,
				DYNA_COMP_SRZ, STRING_SRZ);
		HColumn<DynamicComposite, String> hCol2 = HFactory.createColumn(name2, val2, ttl,
				DYNA_COMP_SRZ, STRING_SRZ);
		HColumn<DynamicComposite, String> hCol3 = HFactory.createColumn(name3, val3, ttl,
				DYNA_COMP_SRZ, STRING_SRZ);

		when(columnsIterator.hasNext()).thenReturn(true, true, true, false, true, false, false);
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		iterator = new AchillesSliceIterator<Long, DynamicComposite, String>(policy, columnFamily,
				query, start, end, false, count);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, String> h1 = iterator.next();

		assertThat(h1.getName()).isEqualTo(name1);
		assertThat(h1.getValue()).isEqualTo(val1);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, String> h2 = iterator.next();

		assertThat(h2.getName()).isEqualTo(name2);
		assertThat(h2.getValue()).isEqualTo(val2);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, String> h3 = iterator.next();

		assertThat(h3.getName()).isEqualTo(name3);
		assertThat(h3.getValue()).isEqualTo(val3);

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(query).setRange(name2, end, false, count);

		verify(policy, times(2)).loadConsistencyLevelForRead(columnFamily);
		verify(policy, times(2)).reinitDefaultConsistencyLevel();

	}
}
