package info.archinnov.achilles.internal.persistence.operations;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.Row;
import info.archinnov.achilles.internal.persistence.operations.NativeQueryMapper;
import info.archinnov.achilles.internal.persistence.operations.TypedMapIterator;
import info.archinnov.achilles.type.TypedMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Iterator;
import java.util.NoSuchElementException;

@RunWith(MockitoJUnitRunner.class)
public class TypedMapIteratorTest {

    @InjectMocks
    private TypedMapIterator iterator;

    @Mock
    private NativeQueryMapper mapper;

    @Mock
    private Iterator<Row> sourceIterator;

    @Mock
    private Row row;

    @Before
    public void setUp() {
        iterator.mapper = mapper;
    }

    @Test
    public void should_return_has_next() throws Exception {
        //Given
        when(sourceIterator.hasNext()).thenReturn(true);

        //Then
        assertThat(iterator.hasNext()).isTrue();
    }

    @Test
    public void should_fetch_next_row() throws Exception {
        //Given
        final TypedMap typedMap = new TypedMap();
        when(sourceIterator.hasNext()).thenReturn(true);
        when(sourceIterator.next()).thenReturn(row);
        when(mapper.mapRow(row)).thenReturn(typedMap);

        //Then
        assertThat(iterator.next()).isSameAs(typedMap);
    }

    @Test(expected = NoSuchElementException.class)
    public void should_throw_exception_if_fetching_no_more_row() throws Exception {
        //Given
        when(sourceIterator.hasNext()).thenReturn(false);

        //Then
        iterator.next();
    }
}