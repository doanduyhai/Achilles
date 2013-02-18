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

import mapping.entity.TweetMultiKey;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * KeyValueIteratorForEntityTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeyValueIteratorForDynamicCompositeTest
{

	@InjectMocks
	private KeyValueIteratorForDynamicComposite<TweetMultiKey, String> iterator;

	@Mock
	private AchillesSliceIterator<?, DynamicComposite, String> achillesSliceIterator;

	@Mock
	private List<Method> componentSetters;

	@Mock
	private PropertyMeta<TweetMultiKey, String> multiKeyWideMapMeta;

	@Mock
	private KeyValueFactory factory = new KeyValueFactory();

	@Mock
	private MultiKeyProperties multiKeyProperties;

	@Before
	public void setUp()
	{
		iterator.factory = factory;
		when(multiKeyWideMapMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_give_next_keyvalue() throws Exception
	{
		HColumn<DynamicComposite, String> column = mock(HColumn.class);
		KeyValue<TweetMultiKey, String> keyValue = mock(KeyValue.class);

		when(achillesSliceIterator.hasNext()).thenReturn(true);
		when(achillesSliceIterator.next()).thenReturn((HColumn) column);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(TweetMultiKey.class);
		when(multiKeyProperties.getComponentSetters()).thenReturn(componentSetters);

		when(factory.createKeyValueForDynamicComposite(multiKeyWideMapMeta, column)).thenReturn(
				keyValue);

		KeyValue<TweetMultiKey, String> expected = iterator.next();

		assertThat(expected).isSameAs(keyValue);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_give_next_key() throws Exception
	{
		HColumn<DynamicComposite, String> column = mock(HColumn.class);
		TweetMultiKey key = mock(TweetMultiKey.class);

		when(achillesSliceIterator.hasNext()).thenReturn(true);
		when(achillesSliceIterator.next()).thenReturn((HColumn) column);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(TweetMultiKey.class);
		when(multiKeyProperties.getComponentSetters()).thenReturn(componentSetters);

		when(factory.createKeyForDynamicComposite(multiKeyWideMapMeta, column)).thenReturn(key);

		TweetMultiKey expected = iterator.nextKey();

		assertThat(expected).isSameAs(key);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_give_next_value() throws Exception
	{
		HColumn<DynamicComposite, String> column = mock(HColumn.class);
		String value = "value";

		when(achillesSliceIterator.hasNext()).thenReturn(true);
		when(achillesSliceIterator.next()).thenReturn((HColumn) column);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(TweetMultiKey.class);
		when(multiKeyProperties.getComponentSetters()).thenReturn(componentSetters);

		when(factory.createValueForDynamicComposite(multiKeyWideMapMeta, column)).thenReturn(value);

		String expected = iterator.nextValue();

		assertThat(expected).isSameAs(value);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_give_next_ttl() throws Exception
	{
		HColumn<DynamicComposite, String> column = mock(HColumn.class);
		Integer ttl = 5464;

		when(achillesSliceIterator.hasNext()).thenReturn(true);
		when(achillesSliceIterator.next()).thenReturn((HColumn) column);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(TweetMultiKey.class);
		when(multiKeyProperties.getComponentSetters()).thenReturn(componentSetters);

		when(factory.createTtlForDynamicComposite(column)).thenReturn(ttl);

		Integer expected = iterator.nextTtl();

		assertThat(expected).isSameAs(ttl);
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
