package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.internal.metadata.holder.PropertyType.PARTITION_KEY;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.LIST;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.MAP;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SET;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaValuesTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CompoundPKProperties compoundPKProperties;

    @Mock
    private ReflectionInvoker invoker;

    private PropertyMetaValues view;

    @Before
    public void setUp() {
        view = new PropertyMetaValues(meta);
        view.invoker = invoker;
    }

    @Test
    public void should_get_primary_key() throws Exception {
        //Given
        Object entity = new Object();
        Long id = 10L;
        when(meta.type()).thenReturn(PARTITION_KEY);
        when(invoker.getPrimaryKey(entity, meta)).thenReturn(id);

        //When
        final Object actual = view.getPrimaryKey(entity);

        //Then
        assertThat(actual).isSameAs(id);
    }

    @Test
    public void should_instantiate() throws Exception {
        //Given
        CompleteBean instance = new CompleteBean();
        when(meta.<CompleteBean>getValueClass()).thenReturn(CompleteBean.class);
        when(invoker.instantiate(CompleteBean.class)).thenReturn(instance);

        //When
        final Object actual = view.instantiate();

        //Then
        assertThat(actual).isSameAs(instance);
    }

    @Test
    public void should_get_value_from_field() throws Exception {
        //Given
        CompleteBean entity = new CompleteBean();
        Field name = CompleteBean.class.getDeclaredField("name");

        when(meta.getField()).thenReturn(name);
        when(invoker.getValueFromField(entity, name)).thenReturn("DuyHai");

        //When
        final Object actual = view.getValueFromField(entity);

        //Then
        assertThat(actual).isEqualTo("DuyHai");
    }

    @Test
    public void should_set_value_to_field() throws Exception {
        //Given
        CompleteBean entity = new CompleteBean();
        Field name = CompleteBean.class.getDeclaredField("name");

        when(meta.getField()).thenReturn(name);

        //When
        view.setValueToField(entity, "DuyHai");

        //Then
        verify(invoker).setValueToField(entity, name, "DuyHai");
    }

    @Test
    public void should_return_empty_list_as_collection() throws Exception {
        //Given
        when(meta.isEmptyCollectionAndMapIfNull()).thenReturn(true);
        when(meta.type()).thenReturn(LIST);

        //When
        final Object actual = view.nullValueForCollectionAndMap();

        //Then
        assertThat(actual).isInstanceOf(ArrayList.class);
        assertThat((List)actual).isEmpty();
    }

    @Test
    public void should_return_empty_set_as_collection() throws Exception {
        //Given
        when(meta.isEmptyCollectionAndMapIfNull()).thenReturn(true);
        when(meta.type()).thenReturn(SET);

        //When
        final Object actual = view.nullValueForCollectionAndMap();

        //Then
        assertThat(actual).isInstanceOf(HashSet.class);
        assertThat((Set)actual).isEmpty();
    }

    @Test
    public void should_return_empty_map_as_collection() throws Exception {
        //Given
        when(meta.isEmptyCollectionAndMapIfNull()).thenReturn(true);
        when(meta.type()).thenReturn(MAP);

        //When
        final Object actual = view.nullValueForCollectionAndMap();

        //Then
        assertThat(actual).isInstanceOf(HashMap.class);
        assertThat((Map)actual).isEmpty();
    }

    @Test
    public void should_force_load() throws Exception {
        //Given
        Object entity = new Object();
        Method getter = CompleteBean.class.getDeclaredMethod("getName");
        when(meta.getGetter()).thenReturn(getter);
        when(invoker.getValueFromField(entity, getter)).thenReturn("DuyHai");

        //When
        final Object actual = view.forceLoad(entity);

        //Then
        assertThat(actual).isEqualTo("DuyHai");
    }
}