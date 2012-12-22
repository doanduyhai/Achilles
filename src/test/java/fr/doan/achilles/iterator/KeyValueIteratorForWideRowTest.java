package fr.doan.achilles.iterator;

import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;

/**
 * KeyValueIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeyValueIteratorForWideRowTest
{

	@InjectMocks
	private KeyValueIteratorForWideRow<Integer, String> iterator;

	@Mock
	private KeyValueFactory factory;

	@Mock
	private ColumnSliceIterator<Long, Integer, String> columnSliceIterator;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Test
	public void should_has_next() throws Exception
	{
		when(columnSliceIterator.hasNext()).thenReturn(true, true, false);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_give_next_element() throws Exception
	{
		KeyValue<Integer, String> keyValue = mock(KeyValue.class);
		HColumn<Integer, String> hColumn = new HColumnImpl<Integer, String>(INT_SRZ, STRING_SRZ);
		hColumn.setName(123);
		hColumn.setValue("test");
		hColumn.setTtl(1);

		when(columnSliceIterator.hasNext()).thenReturn(true, false);
		when(columnSliceIterator.next()).thenReturn(hColumn);
		when(factory.create(123, "test", 1)).thenReturn(keyValue);
		when(wideMapMeta.getValue("test")).thenReturn("test");

		KeyValue<Integer, String> result = iterator.next();

		assertThat(result).isSameAs(keyValue);
	}

	@Test(expected = NoSuchElementException.class)
	public void should_exception_when_no_more_element() throws Exception
	{
		when(columnSliceIterator.hasNext()).thenReturn(false);
		iterator.next();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_remove_called() throws Exception
	{
		iterator.remove();
	}

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(iterator, "factory", factory);
	}
}
