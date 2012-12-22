package fr.doan.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
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

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.holder.KeyValue;

/**
 * KeyValueIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class DynamicCompositeKeyValueIteratorTest
{

	@InjectMocks
	private DynamicCompositeKeyValueIterator<Integer, String> iterator;

	@Mock
	private ColumnSliceIterator<?, DynamicComposite, String> columnSliceIterator;

	@Mock
	private Serializer<?> keySerializer;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Test
	public void should_has_next() throws Exception
	{
		when(columnSliceIterator.hasNext()).thenReturn(true);
		assertThat(iterator.hasNext()).isTrue();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_call_remove() throws Exception
	{
		iterator.remove();
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

		when(wideMapMeta.getValue("val")).thenReturn("val");
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
