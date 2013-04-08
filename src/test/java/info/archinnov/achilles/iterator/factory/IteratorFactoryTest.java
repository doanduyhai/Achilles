package info.archinnov.achilles.iterator.factory;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.KeyValueIteratorForComposite;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;

import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * IteratorFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class IteratorFactoryTest
{
	private IteratorFactory factory = new IteratorFactory();

	@Mock
	private AchillesSliceIterator<?, Composite, String> columnSliceComposite;

	@Mock
	private AchillesSliceIterator<?, DynamicComposite, String> columnSliceDynamicComposite;

	@Mock
	private AchillesJoinSliceIterator<Long, Composite, String, Long, Integer, UserBean> joinColumnSliceComposite;

	@Mock
	private AchillesJoinSliceIterator<Long, DynamicComposite, String, Long, Integer, UserBean> joinColumnSliceDynamicComposite;

	@Mock
	private Iterator<HCounterColumn<DynamicComposite>> counterSliceIterator;

	@Mock
	private List<Method> componentSetters;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private PropertyMeta<Integer, UserBean> joinWideMapMeta;

	@Mock
	private PropertyMeta<Integer, String> multiKeyWideMapMeta;

	@Mock
	private PropertyMeta<Integer, Long> counterWideMapMeta;

	@Mock
	private PersistenceContext<Long> context;

	@Test
	public void should_create_composite_key_value_iterator() throws Exception
	{
		when(wideMapMeta.isSingleKey()).thenReturn(true);
		when(columnSliceComposite.hasNext()).thenReturn(true, false, true);

		KeyValueIterator<Integer, String> iterator = factory.createKeyValueIteratorForComposite(
				context, columnSliceComposite, wideMapMeta);

		assertThat(iterator).isExactlyInstanceOf(KeyValueIteratorForComposite.class);
		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_create_dynamic_composite_key_value_iterator() throws Exception
	{
		KeyValueIterator<Integer, String> iterator = factory
				.createKeyValueIteratorForDynamicComposite(context, columnSliceDynamicComposite,
						wideMapMeta);

		when(columnSliceDynamicComposite.hasNext()).thenReturn(true, false, true);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_create_join_composite_key_value_iterator() throws Exception
	{
		when(wideMapMeta.isSingleKey()).thenReturn(true);
		when(joinColumnSliceComposite.hasNext()).thenReturn(true, false, true);

		KeyValueIterator<Integer, UserBean> iterator = factory
				.createKeyValueJoinIteratorForComposite(context, joinColumnSliceComposite,
						joinWideMapMeta);

		assertThat(iterator).isExactlyInstanceOf(KeyValueIteratorForComposite.class);
		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_create_join_dynamic_composite_key_value_iterator() throws Exception
	{
		KeyValueIterator<Integer, UserBean> iterator = factory
				.createKeyValueJoinIteratorForDynamicComposite(context,
						joinColumnSliceDynamicComposite, joinWideMapMeta);

		when(joinColumnSliceDynamicComposite.hasNext()).thenReturn(true, false, true);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_create_counter_key_value_iterator() throws Exception
	{
		KeyValueIterator<Integer, Long> iterator = factory
				.createCounterKeyValueIteratorForDynamicComposite(counterSliceIterator,
						counterWideMapMeta);

		when(counterSliceIterator.hasNext()).thenReturn(true, false, true);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}
}
