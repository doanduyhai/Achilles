package info.archinnov.achilles.internal.metadata.holder;

import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

@RunWith(MockitoJUnitRunner.class)
public class EntityMetaInterceptorsTest {

    private EntityMeta meta = new EntityMeta();

    private EntityMetaInterceptors view;

    @Before
    public void setUp() {
        view = new EntityMetaInterceptors(meta);
    }

    @Test
    public void should_add_interceptor() throws Exception {
        //Given
        Interceptor<String> interceptor = mock(Interceptor.class);

        //When
        view.addInterceptor(interceptor);

        //Then
        assertThat(view.getInterceptors()).containsExactly(interceptor);
    }

    @Test
    public void should_get_interceptor_for_event() throws Exception {
        //Given
        Interceptor<String> interceptor = mock(Interceptor.class);
        when(interceptor.events()).thenReturn(asList(Event.POST_LOAD, Event.POST_PERSIST));

        //When
        view.addInterceptor(interceptor);

        //Then
        assertThat(view.getInterceptorsForEvent(Event.POST_LOAD)).containsExactly(interceptor);
        assertThat(view.getInterceptorsForEvent(Event.POST_PERSIST)).containsExactly(interceptor);
        assertThat(view.getInterceptorsForEvent(Event.POST_UPDATE)).isEmpty();
    }
}