package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.internal.metadata.holder.EntityMetaBuilder.entityMetaBuilder;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.InsertStrategy;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class EntityMetaOperationsTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    private EntityMetaOperations view;

    @Before
    public void setUp() {
        view = new EntityMetaOperations(meta);
    }

    @Test
    public void should_get_primary_key_from_entity() throws Exception {
        //Given
        Object entity = new Object();
        Long id = RandomUtils.nextLong();

        when(meta.getIdMeta().forValues().getPrimaryKey(entity)).thenReturn(id);

        //When
        final Object actual = view.getPrimaryKey(entity);

        //Then
        assertThat(actual).isSameAs(id);
    }

    @Test
    public void should_instantiate_entity() throws Exception {
        //Given
        when(meta.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);

        //When
        Object actual = view.instanciate();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual).isInstanceOf(CompleteBean.class);
    }

    @Test
    public void should_get_columns_meta_to_load_for_clustered_counter() throws Exception {
        //Given
        PropertyMeta counterMeta = mock(PropertyMeta.class);
        when(meta.structure().isClusteredCounter()).thenReturn(true);
        when(meta.getPropertyMetas().values()).thenReturn(asList(counterMeta));

        //When
        final List<PropertyMeta> actual = view.getColumnsMetaToLoad();

        //Then
        assertThat(actual).containsExactly(counterMeta);
    }

    @Test
    public void should_get_columns_meta_to_load() throws Exception {
        //Given
        PropertyMeta nameMeta = mock(PropertyMeta.class);
        when(meta.structure().isClusteredCounter()).thenReturn(false);
        when(meta.getAllMetasExceptCounters()).thenReturn(asList(idMeta, nameMeta));

        //When
        final List<PropertyMeta> actual = view.getColumnsMetaToLoad();

        //Then
        assertThat(actual).containsExactly(idMeta, nameMeta);
    }

    @Test
    public void should_retrieve_all_property_metas_for_insert() throws Exception {
        //Given
        Object entity = new Object();
        PropertyMeta nameMeta = mock(PropertyMeta.class);
        when(meta.config().getInsertStrategy()).thenReturn(InsertStrategy.ALL_FIELDS);
        when(meta.getAllMetasExceptIdAndCounters()).thenReturn(asList(nameMeta));

        //When
        final List<PropertyMeta> actual = view.retrievePropertyMetasForInsert(entity);

        //Then
        assertThat(actual).containsExactly(nameMeta);
    }

    @Test
    public void should_retrieve_non_null_property_metas_for_insert() throws Exception {
        //Given
        Object entity = new Object();
        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta listMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(meta.config().getInsertStrategy()).thenReturn(InsertStrategy.NOT_NULL_FIELDS);
        when(meta.getAllMetasExceptIdAndCounters()).thenReturn(asList(nameMeta, listMeta));

        when(nameMeta.forValues().getValueFromField(entity)).thenReturn("DuyHai");
        when(listMeta.forValues().getValueFromField(entity)).thenReturn(null);

        //When
        final List<PropertyMeta> actual = view.retrievePropertyMetasForInsert(entity);

        //Then
        assertThat(actual).containsExactly(nameMeta);
    }
}