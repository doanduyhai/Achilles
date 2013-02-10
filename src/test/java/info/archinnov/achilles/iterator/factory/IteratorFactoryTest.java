package info.archinnov.achilles.iterator.factory;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.KeyValueIteratorForComposite;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;

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
	private List<Method> componentSetters;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private PropertyMeta<Integer, String> multiKeyWideMapMeta;

	@Test
	public void should_create_key_value_iterator() throws Exception
	{
		when(wideMapMeta.isSingleKey()).thenReturn(true);
		when(columnSliceComposite.hasNext()).thenReturn(true, false, true);

		KeyValueIterator<Integer, String> iterator = factory.createKeyValueIteratorForComposite(
				columnSliceComposite, wideMapMeta);

		assertThat(iterator).isExactlyInstanceOf(KeyValueIteratorForComposite.class);
		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_create_dynamic_composite_key_value_iterator() throws Exception
	{
		KeyValueIterator<Integer, String> iterator = factory
				.createKeyValueIteratorForDynamicComposite(columnSliceDynamicComposite, wideMapMeta);

		when(columnSliceDynamicComposite.hasNext()).thenReturn(true, false, true);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

}
