package fr.doan.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.entity.metadata.MultiKeyProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;

/**
 * KeyValueIteratorForEntityTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeyValueIteratorForDynamicCompositeTest
{

	@Mock
	private AchillesSliceIterator<?, DynamicComposite, String> achillesSliceIterator;

	@Mock
	private List<Method> componentSetters;

	@Mock
	private PropertyMeta<TweetMultiKey, String> multiKeyWideMapMeta;

	@Mock
	private KeyValueFactory factory = new KeyValueFactory();

	@InjectMocks
	private KeyValueIteratorForDynamicComposite<TweetMultiKey, String> iterator;

	@Mock
	private MultiKeyProperties multiKeyProperties;

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(iterator, "factory", factory);
		when(multiKeyWideMapMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_give_next() throws Exception
	{
		HColumn<DynamicComposite, Object> column = mock(HColumn.class);
		KeyValue<TweetMultiKey, String> keyValue = mock(KeyValue.class);

		when(achillesSliceIterator.hasNext()).thenReturn(true);
		when(achillesSliceIterator.next()).thenReturn((HColumn) column);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(TweetMultiKey.class);
		when(multiKeyProperties.getComponentSetters()).thenReturn(componentSetters);

		when(factory.createKeyValueForDynamicComposite(multiKeyWideMapMeta, column)).thenReturn(keyValue);

		KeyValue<TweetMultiKey, String> expected = iterator.next();

		assertThat(expected).isSameAs(keyValue);
	}

	@Test(expected = NoSuchElementException.class)
	public void should_exception_when_no_more_element() throws Exception
	{
		when(achillesSliceIterator.hasNext()).thenReturn(false);
		iterator.next();
	}

}
