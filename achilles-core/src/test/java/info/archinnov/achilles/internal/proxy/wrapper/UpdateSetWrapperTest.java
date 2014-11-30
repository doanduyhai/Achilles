package info.archinnov.achilles.internal.proxy.wrapper;

import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_SET;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_COLLECTION_OR_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_SET;
import static org.fest.assertions.api.Assertions.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.google.common.collect.Sets;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Method;
import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class UpdateSetWrapperTest {


    private Map<Method, DirtyChecker> dirtyMap;

    private Method setter;

    @Mock
    private PropertyMeta propertyMeta;

    @Mock
    private EntityProxifier proxifier;

    private EntityMeta entityMeta;

    @Before
    public void setUp() throws Exception {
        setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
        when(propertyMeta.type()).thenReturn(PropertyType.LIST);

        entityMeta = new EntityMeta();
        dirtyMap = new HashMap<>();
    }

    @Test
    public void should_mark_dirty_on_element_add() throws Exception {
        UpdateSetWrapper wrapper = prepareSetWrapper();
        when(proxifier.removeProxy("a")).thenReturn("a");
        wrapper.add("a");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(ADD_TO_SET);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawSetChanges()).containsExactly("a");
    }


    @Test
    public void should_mark_dirty_on_add_all() throws Exception {
        UpdateSetWrapper wrapper = prepareSetWrapper();
        Collection<String> list = Arrays.asList("a", "b");

        wrapper.setProxifier(proxifier);

        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.addAll(list);

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(ADD_TO_SET);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawSetChanges()).containsOnly("a", "b");
    }

    @Test
    public void should_not_mark_dirty_on_empty_add_all() throws Exception {
        UpdateSetWrapper wrapper = prepareSetWrapper();
        wrapper.addAll(new HashSet<>());

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_mark_dirty_on_clear() throws Exception {
        UpdateSetWrapper wrapper = prepareSetWrapper();
        wrapper.clear();

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_COLLECTION_OR_MAP);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawSetChanges()).isEmpty();
    }

    @Test
    public void should_mark_dirty_on_remove() throws Exception {
        UpdateSetWrapper wrapper = prepareSetWrapper();
        when(proxifier.removeProxy("a")).thenReturn("a");
        wrapper.remove("a");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_SET);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawSetChanges()).containsOnly("a");
    }

    @Test
    public void should_mark_dirty_on_remove_all() throws Exception {
        UpdateSetWrapper wrapper = prepareSetWrapper();
        wrapper.setProxifier(proxifier);

        Collection<String> list = Arrays.asList("a", "c");
        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.removeAll(list);

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_SET);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawSetChanges()).containsOnly("a", "c");
    }

    private UpdateSetWrapper prepareSetWrapper() {
        UpdateSetWrapper wrapper = new UpdateSetWrapper();
        wrapper.setDirtyMap(dirtyMap);
        wrapper.setSetter(setter);
        wrapper.setPropertyMeta(propertyMeta);
        wrapper.setProxifier(proxifier);
        return wrapper;
    }
}