package info.archinnov.achilles.internal.metadata.holder;

import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.interceptor.AchillesInternalInterceptor;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;

@RunWith(MockitoJUnitRunner.class)
public class EntityMetaInterceptorsTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    private EntityMetaInterceptors view;

    @Mock
    private EntityProxifier proxifier;

    @Before
    public void setUp() {
        view = new EntityMetaInterceptors(meta);
        view.proxifier = proxifier;

    }

    @Test
    public void should_add_interceptor() throws Exception {
        //Given
        Interceptor<String> interceptor = mock(Interceptor.class);
        final ArrayList<Interceptor<?>> interceptors = new ArrayList<>();
        when(meta.getInterceptors()).thenReturn(interceptors);
        //When
        view.addInterceptor(interceptor);

        //Then
        assertThat(view.getInterceptors()).containsExactly(interceptor);
    }

    @Test
    public void should_get_interceptor_for_event() throws Exception {
        //Given
        Interceptor<String> interceptor = mock(Interceptor.class);
        final ArrayList<Interceptor<?>> interceptors = new ArrayList<>();
        when(meta.getInterceptors()).thenReturn(interceptors);
        when(interceptor.events()).thenReturn(asList(Event.POST_LOAD, Event.POST_INSERT));

        //When
        view.addInterceptor(interceptor);

        //Then
        assertThat(view.getInterceptorsForEvent(Event.POST_LOAD)).containsExactly(interceptor);
        assertThat(view.getInterceptorsForEvent(Event.POST_INSERT)).containsExactly(interceptor);
        assertThat(view.getInterceptorsForEvent(Event.POST_UPDATE)).isEmpty();
    }

    @Test
    public void should_intercept_with_user_interceptor() throws Exception {
        //Given

        final Object entity = new Object();
        final Interceptor<Object> interceptor = mock(Interceptor.class);
        final ArrayList<Interceptor<?>> interceptors = new ArrayList<>();
        when(meta.getInterceptors()).thenReturn(interceptors);
        when(interceptor.events()).thenReturn(asList(Event.POST_LOAD, Event.POST_INSERT));

        view.addInterceptor(interceptor);

        when(proxifier.getRealObject(entity)).thenReturn(entity);

        //When
        view.intercept(entity, Event.POST_LOAD);

        //Then
        verify(proxifier).getRealObject(entity);
        verify(interceptor).onEvent(entity);
    }

    @Test
    public void should_intercept_with_internal_interceptor() throws Exception {
        //Given

        final Object entity = new Object();
        final AchillesInternalInterceptor<Object> interceptor = mock(AchillesInternalInterceptor.class);
        final ArrayList<Interceptor<?>> interceptors = new ArrayList<>();
        when(meta.getInterceptors()).thenReturn(interceptors);
        when(interceptor.events()).thenReturn(asList(Event.POST_LOAD, Event.POST_INSERT));

        view.addInterceptor(interceptor);

        when(proxifier.getRealObject(entity)).thenReturn(entity);

        //When
        view.intercept(entity, Event.POST_LOAD);

        //Then
        verify(proxifier,never()).getRealObject(entity);
        verify(interceptor).onEvent(entity);
    }
}