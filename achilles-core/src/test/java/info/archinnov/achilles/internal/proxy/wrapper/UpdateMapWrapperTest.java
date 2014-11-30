package info.archinnov.achilles.internal.proxy.wrapper;

import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.*;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_MAP;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.data.MapEntry.entry;
import static org.mockito.Mockito.*;

import com.google.common.collect.Lists;
import info.archinnov.achilles.internal.context.PersistenceContext;
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
public class UpdateMapWrapperTest {


    private Map<Method, DirtyChecker> dirtyMap;

    private Method setter;

    @Mock
    private PropertyMeta propertyMeta;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private PersistenceContext context;

    @Before
    public void setUp() throws Exception {
        setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
        when(propertyMeta.type()).thenReturn(PropertyType.MAP);
        dirtyMap = new HashMap<>();
    }

    @Test
    public void should_mark_dirty_when_clear_on_full_map() throws Exception {
        UpdateMapWrapper wrapper = prepareMapWrapper();

        wrapper.clear();

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_COLLECTION_OR_MAP);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawMapChanges()).isEmpty();
    }

    @Test
    public void should_mark_dirty_on_put() throws Exception {
        UpdateMapWrapper wrapper = prepareMapWrapper();
        when(proxifier.removeProxy("sdfs")).thenReturn("sdfs");
        when(proxifier.removeProxy(4)).thenReturn(4);

        wrapper.put(4, "sdfs");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(ADD_TO_MAP);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);

        assertThat(changeSet.getRawMapChanges()).hasSize(1).containsKey(4)
                .containsValue("sdfs");
    }

    @Test
    public void should_mark_dirty_on_put_all() throws Exception {
        // Given
        UpdateMapWrapper wrapper = prepareMapWrapper();

        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(1, "FR");
        map.put(2, "Paris");

        when(proxifier.removeProxy(1)).thenReturn(1);
        when(proxifier.removeProxy("FR")).thenReturn("FR");
        when(proxifier.removeProxy(2)).thenReturn(2);
        when(proxifier.removeProxy("Paris")).thenReturn("Paris");

        // When
        wrapper.putAll(map);

        // Then
        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(ADD_TO_MAP);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);

        assertThat(changeSet.getRawMapChanges()).hasSize(2)
                .contains(entry(1, "FR"),entry(2,"Paris"));
    }

    @Test
    public void should_mark_dirty_on_remove_existing() throws Exception {
        UpdateMapWrapper wrapper = prepareMapWrapper();
        when(proxifier.removeProxy(1)).thenReturn(1);
        wrapper.remove(1);

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_MAP);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);

        assertThat(changeSet.getRawMapChanges()).hasSize(1)
                .contains(entry(1, null));
    }

    private UpdateMapWrapper prepareMapWrapper() {
        UpdateMapWrapper wrapper = new UpdateMapWrapper();
        wrapper.setDirtyMap(dirtyMap);
        wrapper.setSetter(setter);
        wrapper.setPropertyMeta(propertyMeta);
        wrapper.setProxifier(proxifier);
        return wrapper;
    }
}