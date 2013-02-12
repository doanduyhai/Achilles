package info.archinnov.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.holder.KeyValue;
import info.archinnov.achilles.holder.factory.KeyValueFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import parser.entity.CorrectMultiKey;

/**
 * KeyValueIteratorForCompositeTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeyValueIteratorForCompositeTest
{

	@InjectMocks
	private KeyValueIteratorForComposite<CorrectMultiKey, String> iterator;

	@Mock
	private AchillesSliceIterator<CorrectMultiKey, Composite, String> achillesSliceIterator;

	@Mock
	private List<Method> componentSetters;

	@Mock
	private PropertyMeta<CorrectMultiKey, String> multiKeyWideMapMeta;

	@Mock
	private MultiKeyProperties multiKeyProperties;

	@Mock
	private KeyValueFactory factory;

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(iterator, "factory", factory);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectMultiKey.class);
		when(multiKeyWideMapMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
	}

	@Test
	public void should_has_next() throws Exception
	{
		when(achillesSliceIterator.hasNext()).thenReturn(true, true, false);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_give_next_keyvalue() throws Exception
	{
		KeyValue<CorrectMultiKey, String> keyValue = mock(KeyValue.class);
		HColumn<Composite, String> hColumn = mock(HColumn.class);

		when(achillesSliceIterator.hasNext()).thenReturn(true, false);
		when(achillesSliceIterator.next()).thenReturn(hColumn);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectMultiKey.class);
		when(multiKeyProperties.getComponentSetters()).thenReturn(componentSetters);

		when(factory.createKeyValueForComposite(multiKeyWideMapMeta, hColumn)).thenReturn(keyValue);

		KeyValue<CorrectMultiKey, String> result = iterator.next();

		assertThat(result).isSameAs(keyValue);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_give_next_key() throws Exception
	{
		CorrectMultiKey key = mock(CorrectMultiKey.class);
		HColumn<Composite, String> hColumn = mock(HColumn.class);

		when(achillesSliceIterator.hasNext()).thenReturn(true, false);
		when(achillesSliceIterator.next()).thenReturn(hColumn);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectMultiKey.class);
		when(multiKeyProperties.getComponentSetters()).thenReturn(componentSetters);

		when(factory.createKeyForComposite(multiKeyWideMapMeta, hColumn)).thenReturn(key);

		CorrectMultiKey result = iterator.nextKey();

		assertThat(result).isSameAs(key);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_give_next_value() throws Exception
	{
		String value = "value";
		HColumn<Composite, String> hColumn = mock(HColumn.class);

		when(achillesSliceIterator.hasNext()).thenReturn(true, false);
		when(achillesSliceIterator.next()).thenReturn(hColumn);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectMultiKey.class);
		when(multiKeyProperties.getComponentSetters()).thenReturn(componentSetters);

		when(factory.createValueForComposite(multiKeyWideMapMeta, hColumn)).thenReturn(value);

		String result = iterator.nextValue();

		assertThat(result).isSameAs(value);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_give_next_ttl() throws Exception
	{
		Integer ttl = 123;
		HColumn<Composite, String> hColumn = mock(HColumn.class);

		when(achillesSliceIterator.hasNext()).thenReturn(true, false);
		when(achillesSliceIterator.next()).thenReturn(hColumn);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectMultiKey.class);
		when(multiKeyProperties.getComponentSetters()).thenReturn(componentSetters);

		when(factory.createTtlForComposite(hColumn)).thenReturn(ttl);

		Integer result = iterator.nextTtl();

		assertThat(result).isSameAs(ttl);
	}

	@Test(expected = NoSuchElementException.class)
	public void should_exception_when_no_more_element() throws Exception
	{
		when(achillesSliceIterator.hasNext()).thenReturn(false);
		iterator.next();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_remove_called() throws Exception
	{
		iterator.remove();
	}
}
