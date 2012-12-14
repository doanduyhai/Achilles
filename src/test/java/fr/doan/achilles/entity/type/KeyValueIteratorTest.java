package fr.doan.achilles.entity.type;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * KeyValueIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class KeyValueIteratorTest
{

	@InjectMocks
	private KeyValueIterator<Integer, String> iterator;

	@Mock
	private ColumnSliceIterator<?, DynamicComposite, String> columnSliceIterator;

	@Mock
	private Serializer<?> keySerializer;

	@Test
	public void should_has_next() throws Exception
	{
		when(columnSliceIterator.hasNext()).thenReturn(true);
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test
	public void should_remove() throws Exception
	{
		iterator.remove();
		verify(columnSliceIterator).remove();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_give_next() throws Exception
	{
		HColumn<DynamicComposite, String> column = mock(HColumn.class);
		DynamicComposite comp = mock(DynamicComposite.class);

		when(columnSliceIterator.hasNext()).thenReturn(true);
		when(columnSliceIterator.next()).thenReturn(column);
		when(column.getName()).thenReturn(comp);

		doReturn(12).when(comp).get(2, keySerializer);
		when(column.getValue()).thenReturn("val");
		when(column.getTtl()).thenReturn(120);

		KeyValue<Integer, String> keyValue = iterator.next();

		assertThat(keyValue.getKey()).isEqualTo(12);
		assertThat(keyValue.getValue()).isEqualTo("val");
		assertThat(keyValue.getTtl()).isEqualTo(120);
	}

	@Test(expected = NoSuchElementException.class)
	public void should_exception_when_no_next() throws Exception
	{
		when(columnSliceIterator.hasNext()).thenReturn(false);
		iterator.next();
	}

}
