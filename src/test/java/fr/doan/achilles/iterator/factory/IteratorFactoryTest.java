package fr.doan.achilles.iterator.factory;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.metadata.WideMapMeta;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.iterator.KeyValueIteratorForWideRow;
import fr.doan.achilles.iterator.MultiKeyKeyValueIteratorForWideRow;

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
	private ColumnSliceIterator<?, Composite, Object> columnSliceComposite;

	@Mock
	private ColumnSliceIterator<?, DynamicComposite, Object> columnSliceDynamicComposite;

	@Mock
	private List<Method> componentSetters;

	@Mock
	private WideMapMeta<Integer, String> wideMapMeta;

	@Mock
	private MultiKeyWideMapMeta<Integer, String> multiKeyWideMapMeta;

	@Test
	public void should_create_key_value_iterator() throws Exception
	{
		KeyValueIteratorForWideRow<Integer, String> iterator = factory
				.createKeyValueIteratorForWideRow(columnSliceComposite, wideMapMeta);

		when(columnSliceComposite.hasNext()).thenReturn(true, false, true);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_create_multikey_key_value_iterator() throws Exception
	{
		MultiKeyKeyValueIteratorForWideRow<Integer, String> iterator = factory
				.createMultiKeyKeyValueIteratorForWideRow(columnSliceComposite, componentSetters,
						multiKeyWideMapMeta);

		when(columnSliceComposite.hasNext()).thenReturn(true, false, true);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_create_dynamic_composite_key_value_iterator() throws Exception
	{
		KeyValueIterator<Integer, String> iterator = factory.createKeyValueIteratorForEntity(
				columnSliceDynamicComposite, wideMapMeta);

		when(columnSliceDynamicComposite.hasNext()).thenReturn(true, false, true);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_create_multikey_dynamic_composite_key_value_iterator() throws Exception
	{
		KeyValueIterator<Integer, String> iterator = factory.createKeyValueIteratorForEntity(
				columnSliceDynamicComposite, multiKeyWideMapMeta);

		when(columnSliceDynamicComposite.hasNext()).thenReturn(true, false, true);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.hasNext()).isTrue();
	}
}
