package info.archinnov.achilles.iterator.factory;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.ThriftAbstractSliceIterator;
import info.archinnov.achilles.iterator.ThriftJoinSliceIterator;
import info.archinnov.achilles.iterator.ThriftKeyValueIteratorImpl;
import info.archinnov.achilles.iterator.ThriftSliceIterator;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.KeyValueIterator;

import java.lang.reflect.Method;
import java.util.List;

import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * ThriftIteratorFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftIteratorFactoryTest
{
	private ThriftIteratorFactory factory = new ThriftIteratorFactory();

	@Mock
	private ThriftSliceIterator<?, String> columnSliceComposite;

	@Mock
	private ThriftSliceIterator<?, String> columnSliceDynamicComposite;

	@Mock
	private ThriftJoinSliceIterator<String, Integer, UserBean> joinColumnSliceComposite;

	@Mock
	private ThriftJoinSliceIterator<String, Integer, UserBean> joinColumnSliceDynamicComposite;

	@Mock
	private ThriftAbstractSliceIterator<HCounterColumn<Composite>> counterSliceIterator;

	@Mock
	private List<Method> componentSetters;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private PropertyMeta<Integer, UserBean> joinWideMapMeta;

	@Mock
	private PropertyMeta<Integer, String> multiKeyWideMapMeta;

	@Mock
	private PropertyMeta<Integer, Counter> counterWideMapMeta;

	@Mock
	private ThriftPersistenceContext context;

	@Test
	public void should_create_composite_key_value_iterator() throws Exception
	{
		when(wideMapMeta.isSingleKey()).thenReturn(true);
		when(columnSliceComposite.hasNext()).thenReturn(true, false, true);

		KeyValueIterator<Integer, String> iterator = factory.createKeyValueIterator(context,
				columnSliceComposite, wideMapMeta);

		assertThat(iterator).isExactlyInstanceOf(ThriftKeyValueIteratorImpl.class);
		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_create_dynamic_composite_key_value_iterator() throws Exception
	{
		KeyValueIterator<Integer, String> iterator = factory.createKeyValueIterator(context,
				columnSliceDynamicComposite, wideMapMeta);

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

		KeyValueIterator<Integer, UserBean> iterator = factory.createJoinKeyValueIterator(context,
				joinColumnSliceComposite, joinWideMapMeta);

		assertThat(iterator).isExactlyInstanceOf(ThriftKeyValueIteratorImpl.class);
		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_create_join_dynamic_composite_key_value_iterator() throws Exception
	{
		KeyValueIterator<Integer, UserBean> iterator = factory.createJoinKeyValueIterator(context,
				joinColumnSliceDynamicComposite, joinWideMapMeta);

		when(joinColumnSliceDynamicComposite.hasNext()).thenReturn(true, false, true);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_create_counter_key_value_iterator() throws Exception
	{
		KeyValueIterator<Integer, Counter> iterator = factory.createCounterKeyValueIterator(
				context, counterSliceIterator, counterWideMapMeta);

		when(counterSliceIterator.hasNext()).thenReturn(true, false, true);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}
}
