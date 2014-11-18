package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.internal.metadata.holder.PropertyType.COUNTER;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.EMBEDDED_ID;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.LIST;
import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_ONE;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaStructureTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta meta;

    private PropertyMetaStructure view;

    @Before
    public void setUp() {
        view = new PropertyMetaStructure(meta);
    }

    @Test
    public void should_return_is_embedded_id() throws Exception {
        //Given
        when(meta.type()).thenReturn(EMBEDDED_ID);

        //When

        //Then
        assertThat(view.isEmbeddedId()).isTrue();
    }

    @Test
    public void should_return_is_static_column() throws Exception {
        //Given
        when(meta.isStaticColumn()).thenReturn(true);

        //When

        //Then
        assertThat(view.isStaticColumn()).isTrue();
    }


    @Test
    public void should_return_is_counter() throws Exception {
        //Given
        when(meta.type()).thenReturn(COUNTER);

        //When

        //Then
        assertThat(view.isCounter()).isTrue();
    }


    @Test
    public void should_return_is_indexed() throws Exception {
        //Given
        when(meta.getIndexProperties()).thenReturn(new IndexProperties("name","name"));

        //When

        //Then
        assertThat(view.isIndexed()).isTrue();
    }

    @Test
    public void should_return_is_collection_and_map() throws Exception {
        //Given
        when(meta.type()).thenReturn(LIST);

        //When

        //Then
        assertThat(view.isCollectionAndMap()).isTrue();
    }


}