package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.serializer.SerializerUtils.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import info.archinnov.achilles.entity.type.WideMap.OrderingMode;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesSliceIterator;
import info.archinnov.achilles.iterator.KeyValueIteratorImpl;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesJpaEntityInterceptor;
import java.util.List;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * ExternalWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class WideMapWrapperTest {
    @InjectMocks
    private WideMapWrapper<Long, Integer, String> wrapper;

    @Mock
    private ThriftPersistenceContext<Long> context;

    @Mock
    private ThriftGenericWideRowDao<Long, String> dao;

    @Mock
    private PropertyMeta<Integer, String> wideMapMeta;

    @Mock
    private CompositeHelper compositeHelper;

    @Mock
    private KeyValueFactory keyValueFactory;

    @Mock
    private IteratorFactory iteratorFactory;

    @Mock
    private CompositeFactory compositeFactory;

    private Long id;

    private Composite comp = new Composite();

    @Mock
    private AchillesJpaEntityInterceptor<Long> interceptor;

    @Mock
    private Mutator<Long> mutator;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        when(wideMapMeta.getExternalCFName()).thenReturn("external_cf");
        when((Serializer<Long>) wideMapMeta.getIdSerializer()).thenReturn(LONG_SRZ);
        when(wideMapMeta.getKeySerializer()).thenReturn(INT_SRZ);
        when(compositeFactory.createBaseComposite(wideMapMeta, 12)).thenReturn(comp);
        when(context.getWideRowMutator("external_cf")).thenReturn(mutator);

    }

    @Test
    public void should_get_value() throws Exception {
        Composite comp = new Composite();
        when(compositeFactory.createBaseComposite(wideMapMeta, 12)).thenReturn(comp);
        when(dao.getValue(id, comp)).thenReturn("test");
        when(wideMapMeta.castValue("test")).thenReturn("test");

        Object expected = wrapper.get(12);

        assertThat(expected).isEqualTo("test");
    }

    @Test
    public void should_get_null_value() throws Exception {
        Composite comp = new Composite();
        when(compositeFactory.createBaseComposite(wideMapMeta, 12)).thenReturn(comp);
        when(dao.getValue(id, comp)).thenReturn(null);

        assertThat(wrapper.get(12)).isNull();
    }

    @Test
    public void should_insert_value() throws Exception {
        when(wideMapMeta.writeValueAsSupportedTypeOrString("test")).thenReturn("test");
        wrapper.insert(12, "test");
        verify(dao).setValueBatch(id, comp, "test", mutator);
        verify(context).flush();
    }

    @Test
    public void should_insert_value_with_ttl() throws Exception {
        when(wideMapMeta.writeValueAsSupportedTypeOrString("test")).thenReturn("test");
        wrapper.insert(12, "test", 452);
        verify(dao).setValueBatch(id, comp, "test", 452, mutator);
        verify(context).flush();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void should_find_keyvalues_range() throws Exception {
        List<HColumn<Composite, String>> hColumns = mock(List.class);
        List<KeyValue<Integer, String>> keyValues = mock(List.class);
        Composite startComp = new Composite();
        Composite endComp = new Composite();

        when(
                compositeFactory.createForQuery(wideMapMeta, 12, 15, BoundingMode.INCLUSIVE_BOUNDS,
                        OrderingMode.ASCENDING)) //
                .thenReturn(new Composite[] { startComp, endComp });

        when(dao.findRawColumnsRange(id, startComp, endComp, 10, false)).thenReturn(hColumns);
        when(keyValueFactory.createKeyValueList(context, wideMapMeta, (List) hColumns)).thenReturn(keyValues)
                .thenReturn(keyValues);

        List<KeyValue<Integer, String>> expected = wrapper.find(12, 15, 10);
        assertThat(expected).isSameAs(keyValues);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void should_find_values_range() throws Exception {
        List<HColumn<Composite, String>> hColumns = mock(List.class);
        List<String> keyValues = mock(List.class);
        Composite startComp = new Composite();
        Composite endComp = new Composite();

        when(
                compositeFactory.createForQuery(wideMapMeta, 12, 15, BoundingMode.INCLUSIVE_BOUNDS,
                        OrderingMode.ASCENDING)) //
                .thenReturn(new Composite[] { startComp, endComp });

        when(dao.findRawColumnsRange(id, startComp, endComp, 10, false)).thenReturn(hColumns);
        when(keyValueFactory.createValueList(wideMapMeta, (List) hColumns)).thenReturn(keyValues).thenReturn(
                keyValues);

        List<String> expected = wrapper.findValues(12, 15, 10);
        assertThat(expected).isSameAs(keyValues);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void should_find_keys_range() throws Exception {
        List<HColumn<Composite, String>> hColumns = mock(List.class);
        List<Integer> keyValues = mock(List.class);
        Composite startComp = new Composite();
        Composite endComp = new Composite();

        when(
                compositeFactory.createForQuery(wideMapMeta, 12, 15, BoundingMode.INCLUSIVE_BOUNDS,
                        OrderingMode.ASCENDING)) //
                .thenReturn(new Composite[] { startComp, endComp });

        when(dao.findRawColumnsRange(id, startComp, endComp, 10, false)).thenReturn(hColumns);
        when(keyValueFactory.createKeyList(wideMapMeta, (List) hColumns)).thenReturn(keyValues).thenReturn(keyValues);

        List<Integer> expected = wrapper.findKeys(12, 15, 10);
        assertThat(expected).isSameAs(keyValues);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_get_iterator() throws Exception {
        KeyValueIteratorImpl<Long, Integer, String> keyValues = mock(KeyValueIteratorImpl.class);
        AchillesSliceIterator<Long, String> iterator = mock(AchillesSliceIterator.class);
        Composite startComp = new Composite();
        Composite endComp = new Composite();

        when(
                compositeFactory.createForQuery(wideMapMeta, 12, 15, BoundingMode.INCLUSIVE_START_BOUND_ONLY,
                        OrderingMode.ASCENDING)) //
                .thenReturn(new Composite[] { startComp, endComp });
        when(dao.getColumnsIterator(id, startComp, endComp, false, 10)).thenReturn(iterator);
        when(iteratorFactory.createKeyValueIterator(context, iterator, wideMapMeta)).thenReturn(keyValues);
        KeyValueIterator<Integer, String> expected = wrapper.iterator(12, 15, 10,
                BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.ASCENDING);

        assertThat(expected).isSameAs(keyValues);
    }

    @Test
    public void should_remove() throws Exception {
        Composite comp = new Composite();
        when(compositeFactory.createBaseComposite(wideMapMeta, 12)).thenReturn(comp);

        wrapper.remove(12);

        verify(dao).removeColumnBatch(id, comp, mutator);
        verify(context).flush();
    }

    @Test
    public void should_remove_range() throws Exception {
        Composite startComp = new Composite();
        Composite endComp = new Composite();

        when(
                compositeFactory.createForQuery(wideMapMeta, 12, 15, BoundingMode.INCLUSIVE_END_BOUND_ONLY,
                        OrderingMode.ASCENDING)) //
                .thenReturn(new Composite[] { startComp, endComp });

        wrapper.remove(12, 15, BoundingMode.INCLUSIVE_END_BOUND_ONLY);

        verify(dao).removeColumnRangeBatch(id, startComp, endComp, mutator);
        verify(context).flush();
    }

    @Test
    public void should_remove_first() throws Exception {
        wrapper.removeFirst(3);

        verify(dao).removeColumnRangeBatch(id, null, null, false, 3, mutator);
        verify(context).flush();
    }

    @Test
    public void should_remove_last() throws Exception {
        wrapper.removeLast(7);

        verify(dao).removeColumnRangeBatch(id, null, null, true, 7, mutator);
        verify(context).flush();
    }
}
