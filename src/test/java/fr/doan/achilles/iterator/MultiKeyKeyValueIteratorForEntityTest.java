package fr.doan.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.List;
import java.util.NoSuchElementException;

import mapping.entity.TweetMultiKey;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;

/**
 * MultiKeyValueIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class MultiKeyKeyValueIteratorForEntityTest
{

	@Mock
	private ColumnSliceIterator<?, DynamicComposite, String> columnSliceIterator;

	@Mock
	private List<Method> componentSetters;

	@Mock
	private MultiKeyWideMapMeta<TweetMultiKey, String> multiKeyWideMapMeta;

	@Mock
	private KeyValueFactory factory = new KeyValueFactory();

	@InjectMocks
	private MultiKeyKeyValueIteratorForEntity<TweetMultiKey, String> iterator;

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(iterator, "factory", factory);
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

		when(columnSliceIterator.hasNext()).thenReturn(true);
		when(columnSliceIterator.next()).thenReturn((HColumn) column);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(TweetMultiKey.class);
		when(multiKeyWideMapMeta.getComponentSetters()).thenReturn(componentSetters);

		when(factory.createForWideMap(multiKeyWideMapMeta, column)).thenReturn(keyValue);

		KeyValue<TweetMultiKey, String> expected = iterator.next();

		assertThat(expected).isSameAs(keyValue);
	}

	@Test(expected = NoSuchElementException.class)
	public void should_exception_when_no_more_element() throws Exception
	{
		when(columnSliceIterator.hasNext()).thenReturn(false);
		iterator.next();
	}

}
