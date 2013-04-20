package info.archinnov.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.iterator.AbstractAchillesSliceIterator.IteratorType;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;

import java.util.NoSuchElementException;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * CounterKeyValueIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CounterKeyValueIteratorImplTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private CounterKeyValueIteratorImpl<Integer> iterator;

	@Mock
	private AbstractAchillesSliceIterator<HCounterColumn<Composite>> achillesSliceIterator;

	@Mock
	private KeyValueFactory factory;

	@Mock
	private PropertyMeta<Integer, Long> propertyMeta;

	@Before
	public void setUp()
	{
		when(achillesSliceIterator.type()).thenReturn(IteratorType.ACHILLES_COUNTER_SLICE_ITERATOR);
		Whitebox.setInternalState(iterator, "factory", factory);
	}

	@Test
	public void should_return_hasNext() throws Exception
	{
		when(achillesSliceIterator.hasNext()).thenReturn(true);
		assertThat(iterator.hasNext()).isTrue();

		verify(achillesSliceIterator).hasNext();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_gitve_next_key_value() throws Exception
	{
		KeyValue<Integer, Long> keyValue = new KeyValue<Integer, Long>();
		when(achillesSliceIterator.hasNext()).thenReturn(true);
		HCounterColumn<Composite> hColumn = mock(HCounterColumn.class);
		when(achillesSliceIterator.next()).thenReturn(hColumn);
		when(factory.createCounterKeyValue(propertyMeta, hColumn)).thenReturn(keyValue);

		assertThat(iterator.next()).isSameAs(keyValue);
	}

	@Test(expected = NoSuchElementException.class)
	public void should_exception_when_no_more_key_value() throws Exception
	{
		when(achillesSliceIterator.hasNext()).thenReturn(false);
		iterator.next();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_give_next_key() throws Exception
	{
		when(achillesSliceIterator.hasNext()).thenReturn(true);
		HCounterColumn<Composite> hColumn = mock(HCounterColumn.class);
		when(achillesSliceIterator.next()).thenReturn(hColumn);
		when(factory.createCounterKey(propertyMeta, hColumn)).thenReturn(12);

		assertThat(iterator.nextKey()).isEqualTo(12);
	}

	@Test(expected = NoSuchElementException.class)
	public void should_exception_when_no_more_key() throws Exception
	{
		when(achillesSliceIterator.hasNext()).thenReturn(false);
		iterator.nextKey();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_gitve_next_value() throws Exception
	{
		when(achillesSliceIterator.hasNext()).thenReturn(true);
		HCounterColumn<Composite> hColumn = mock(HCounterColumn.class);
		when(achillesSliceIterator.next()).thenReturn(hColumn);
		when(factory.createCounterValue(propertyMeta, hColumn)).thenReturn(12L);

		assertThat(iterator.nextValue()).isEqualTo(12L);
	}

	@Test(expected = NoSuchElementException.class)
	public void should_exception_when_no_more_value() throws Exception
	{
		when(achillesSliceIterator.hasNext()).thenReturn(false);
		iterator.nextValue();
	}

	@Test
	public void should_exception_when_next_tll() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("Ttl does not exist for counter type");
		iterator.nextTtl();
	}

	@Test
	public void should_exception_when_remove() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception
				.expectMessage("Cannot remove counter value. Please set a its value to 0 instead of removing it");

		iterator.remove();
	}
}
