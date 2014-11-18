package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_ONE;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.type.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaConfigTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta meta;

    private PropertyMetaConfig view;

    @Before
    public void setUp() {
        view = new PropertyMetaConfig(meta);
    }

    @Test
    public void should_return_read_consistency_level() throws Exception {
        //Given
        when(meta.getConsistencyLevels()).thenReturn(Pair.create(ALL, LOCAL_ONE));

        //When

        //Then
        assertThat(view.getReadConsistencyLevel()).isEqualTo(ALL);
    }

    @Test
    public void should_return_write_consistency_level() throws Exception {
        //Given
        when(meta.getConsistencyLevels()).thenReturn(Pair.create(ALL, LOCAL_ONE));

        //When

        //Then
        assertThat(view.getWriteConsistencyLevel()).isEqualTo(LOCAL_ONE);
    }
}