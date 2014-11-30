package info.archinnov.achilles.internal.proxy.wrapper;

import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.*;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Method;
import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class UpdateListWrapperTest {

    private Map<Method, DirtyChecker> dirtyMap;

    private Method setter;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta propertyMeta;

    @Mock
    private EntityProxifier proxifier;


    @Before
    public void setUp() throws Exception {
        setter = CompleteBean.class.getDeclaredMethod("setFriends", List.class);
        dirtyMap = new HashMap<>();
    }

    @Test
    public void should_mark_dirty_on_element_add() throws Exception {
        UpdateListWrapper wrapper = prepareListWrapper();
        when(proxifier.removeProxy("a")).thenReturn("a");
        wrapper.add("a");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(APPEND_TO_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsExactly("a");
    }

    @Test
    public void should_mark_dirty_on_add_all() throws Exception {

        UpdateListWrapper wrapper = prepareListWrapper();
        Collection<String> list = asList("a", "b");

        wrapper.setProxifier(proxifier);

        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.addAll(list);

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(APPEND_TO_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsOnly("a", "b");
    }

    @Test
    public void should_not_mark_dirty_on_empty_add_all() throws Exception {

        UpdateListWrapper wrapper = prepareListWrapper();
        wrapper.addAll(new HashSet<>());

        assertThat(dirtyMap).isEmpty();
    }

    @Test
    public void should_mark_dirty_on_element_prepend() throws Exception {

        UpdateListWrapper listWrapper = prepareListWrapper();
        when(proxifier.removeProxy("a")).thenReturn("a");
        listWrapper.add(0, "a");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(PREPEND_TO_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsOnly("a");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void should_exception_when_adding_at_index() throws Exception {
        //Given
        UpdateListWrapper listWrapper = prepareListWrapper();
        when(proxifier.removeProxy("a")).thenReturn("a");

        //When
        listWrapper.add(1, "a");
    }

    @Test
    public void should_mark_dirty_on_preprend_all() throws Exception {

        UpdateListWrapper listWrapper = prepareListWrapper();
        listWrapper.setProxifier(proxifier);

        Collection<String> list = asList("b", "c");
        when(proxifier.removeProxy(list)).thenReturn(list);

        listWrapper.addAll(0, list);

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(PREPEND_TO_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsExactly("b", "c");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void should_exception_when_adding_all_at_index() throws Exception {
        //Given
        UpdateListWrapper listWrapper = prepareListWrapper();
        when(proxifier.removeProxy("a")).thenReturn("a");

        //When
        listWrapper.addAll(1, asList("a"));
    }

    @Test
    public void should_mark_dirty_on_clear() throws Exception {
        UpdateListWrapper wrapper = prepareListWrapper();
        wrapper.clear();

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_COLLECTION_OR_MAP);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).isEmpty();
    }

    @Test
    public void should_mark_dirty_on_remove() throws Exception {
        UpdateListWrapper wrapper = prepareListWrapper();
        when(proxifier.removeProxy("a")).thenReturn("a");
        wrapper.remove("a");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsOnly("a");
    }

    @Test
    public void should_mark_dirty_on_remove_all() throws Exception {

        UpdateListWrapper wrapper = prepareListWrapper();
        wrapper.setProxifier(proxifier);

        Collection<String> list = Arrays.asList("a", "c");
        when(proxifier.removeProxy(Mockito.<Collection<String>>any())).thenReturn(list);

        wrapper.removeAll(list);

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_LIST);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getRawListChanges()).containsOnly("a", "c");
    }

    @Test
    public void should_mark_dirty_on_remove_at_index() throws Exception {

        UpdateListWrapper listWrapper = prepareListWrapper();
        listWrapper.remove(11);

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(REMOVE_FROM_LIST_AT_INDEX);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getEncodedListChangeAtIndex().getIndex()).isEqualTo(11);
        assertThat(changeSet.getEncodedListChangeAtIndex().getElement()).isNull();
    }

    @Test
    public void should_mark_dirty_on_set() throws Exception {

        UpdateListWrapper listWrapper = prepareListWrapper();
        when(proxifier.removeProxy("d")).thenReturn("d");
        when(propertyMeta.forTranscoding().encodeToCassandra(Arrays.asList("d"))).thenReturn(Arrays.asList("d"));
        listWrapper.set(1, "d");

        DirtyChecker dirtyChecker = dirtyMap.get(setter);
        assertThat(dirtyChecker.getPropertyMeta()).isEqualTo(propertyMeta);
        DirtyCheckChangeSet changeSet = dirtyChecker.getChangeSets().get(0);
        assertThat(changeSet.getChangeType()).isEqualTo(SET_TO_LIST_AT_INDEX);
        assertThat(changeSet.getPropertyMeta()).isEqualTo(propertyMeta);
        assertThat(changeSet.getEncodedListChangeAtIndex().getElement()).isEqualTo("d");
        assertThat(changeSet.getEncodedListChangeAtIndex().getIndex()).isEqualTo(1);
    }

    private UpdateListWrapper prepareListWrapper() {
        UpdateListWrapper listWrapper = new UpdateListWrapper();
        listWrapper.setDirtyMap(dirtyMap);
        listWrapper.setSetter(setter);
        listWrapper.setPropertyMeta(propertyMeta);
        listWrapper.setProxifier(proxifier);
        return listWrapper;
    }

}