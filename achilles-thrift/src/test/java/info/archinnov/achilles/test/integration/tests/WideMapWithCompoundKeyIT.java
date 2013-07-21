package info.archinnov.achilles.test.integration.tests;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.test.integration.entity.EntityWithWideMaps;
import info.archinnov.achilles.test.integration.entity.EntityWithWideMaps.Key;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;
import java.util.List;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * WideMapWithCompoundKeyIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWithCompoundKeyIT
{
    private ThriftGenericWideRowDao dao = ThriftCassandraDaoTest.getColumnFamilyDao(
            "clustered", Long.class, String.class);
    private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

    private EntityWithWideMaps bean;

    private WideMap<Key, String> wideMap;

    private Long primaryKey;

    @Before
    public void setUp()
    {
        primaryKey = RandomUtils.nextLong();
        bean = new EntityWithWideMaps(primaryKey);
        bean = em.merge(bean);
        wideMap = bean.getWideMap();
    }

    @Test
    public void should_insert_values() throws Exception
    {

        wideMap.insert(new Key(1, "name1"), "1-name1");
        wideMap.insert(new Key(1, "name2"), "1-name2");
        wideMap.insert(new Key(2, "name3"), "2-name3");
        wideMap.insert(new Key(3, "name4"), "3-name4");
        wideMap.insert(new Key(3, "name5"), "3-name5");

        Composite startComp = buildComposite(1, EQUAL);
        Composite endComp = buildComposite(3, GREATER_THAN_EQUAL);

        List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(),
                startComp, endComp, false, 20);

        assertThat(columns).hasSize(5);
        assertThat(columns.get(0).right).isEqualTo("1-name1");
        assertThat(columns.get(1).right).isEqualTo("1-name2");
        assertThat(columns.get(2).right).isEqualTo("2-name3");
        assertThat(columns.get(3).right).isEqualTo("3-name4");
        assertThat(columns.get(4).right).isEqualTo("3-name5");
    }

    @Test
    public void should_insert_values_with_ttl() throws Exception
    {
        wideMap.insert(new Key(1, "name1"), "1-name1", 150);

        Composite startComp = buildComposite(1, EQUAL);
        Composite endComp = buildComposite(1, GREATER_THAN_EQUAL);

        List<HColumn<Composite, String>> columns = dao.findRawColumnsRange(bean.getId(),
                startComp, endComp, 10, false);

        assertThat(columns).hasSize(1);
        assertThat(columns.get(0).getTtl()).isEqualTo(150);

    }

    @Test
    public void should_get_value_by_key() throws Exception
    {
        Key key = new Key(1, "name1");
        wideMap.insert(key, "1-name1");

        assertThat(wideMap.get(key)).isEqualTo("1-name1");
    }

    @Test
    public void should_find_values_by_range_exclusive_start_inclusive_end_reverse()
            throws Exception
    {

        wideMap.insert(new Key(1, "name1"), "1-name1");
        wideMap.insert(new Key(1, "name2"), "1-name2");
        wideMap.insert(new Key(2, "name3"), "2-name3");
        wideMap.insert(new Key(3, "name4"), "3-name4");
        wideMap.insert(new Key(3, "name5"), "3-name5");

        List<KeyValue<Key, String>> results = wideMap.find(
                new Key(3, "name5"), new Key(2, "name3"), 10,
                BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

        assertThat(results).hasSize(2);

        assertThat(results.get(0).getKey().getCount()).isEqualTo(3);
        assertThat(results.get(0).getKey().getName()).isEqualTo("name4");

        assertThat(results.get(1).getKey().getCount()).isEqualTo(2);
        assertThat(results.get(1).getKey().getName()).isEqualTo("name3");

    }

    @Test
    public void should_find_values_by_asc_range_with_start_having_null() throws Exception
    {
        wideMap.insert(new Key(1, "name1"), "1-name1");
        wideMap.insert(new Key(1, "name2"), "1-name2");
        wideMap.insert(new Key(2, "name3"), "2-name3");
        wideMap.insert(new Key(3, "name4"), "3-name4");
        wideMap.insert(new Key(3, "name5"), "3-name5");

        List<KeyValue<Key, String>> results = wideMap.find(new Key(1, null),
                new Key(2, "name3"), 10, BoundingMode.INCLUSIVE_BOUNDS,
                OrderingMode.ASCENDING);

        assertThat(results).hasSize(3);

        assertThat(results.get(0).getKey().getCount()).isEqualTo(1);
        assertThat(results.get(0).getKey().getName()).isEqualTo("name1");

        assertThat(results.get(1).getKey().getCount()).isEqualTo(1);
        assertThat(results.get(1).getKey().getName()).isEqualTo("name2");

        assertThat(results.get(2).getKey().getCount()).isEqualTo(2);
        assertThat(results.get(2).getKey().getName()).isEqualTo("name3");
    }

    @Test
    public void should_find_values_by_asc_range_with_end_having_null() throws Exception
    {
        wideMap.insert(new Key(1, "name1"), "1-name1");
        wideMap.insert(new Key(1, "name2"), "1-name2");
        wideMap.insert(new Key(2, "name3"), "2-name3");
        wideMap.insert(new Key(3, "name4"), "3-name4");
        wideMap.insert(new Key(3, "name5"), "3-name5");

        List<KeyValue<Key, String>> results = wideMap.find(
                new Key(1, "name1"), new Key(2, null), 10,
                BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);

        assertThat(results).hasSize(3);

        assertThat(results.get(0).getKey().getCount()).isEqualTo(1);
        assertThat(results.get(0).getKey().getName()).isEqualTo("name1");

        assertThat(results.get(1).getKey().getCount()).isEqualTo(1);
        assertThat(results.get(1).getKey().getName()).isEqualTo("name2");

        assertThat(results.get(2).getKey().getCount()).isEqualTo(2);
        assertThat(results.get(2).getKey().getName()).isEqualTo("name3");
    }

    @Test
    public void should_find_values_by_asc_range_with_start_completely_null() throws Exception
    {
        wideMap.insert(new Key(1, "name1"), "1-name1");
        wideMap.insert(new Key(1, "name2"), "1-name2");
        wideMap.insert(new Key(2, "name3"), "2-name3");
        wideMap.insert(new Key(3, "name4"), "3-name4");
        wideMap.insert(new Key(3, "name5"), "3-name5");

        List<KeyValue<Key, String>> results = wideMap.find(null, new Key(2,
                null), 10, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);

        assertThat(results).hasSize(3);

        assertThat(results.get(0).getKey().getCount()).isEqualTo(1);
        assertThat(results.get(0).getKey().getName()).isEqualTo("name1");

        assertThat(results.get(1).getKey().getCount()).isEqualTo(1);
        assertThat(results.get(1).getKey().getName()).isEqualTo("name2");

        assertThat(results.get(2).getKey().getCount()).isEqualTo(2);
        assertThat(results.get(2).getKey().getName()).isEqualTo("name3");
    }

    @Test
    public void should_iterate() throws Exception
    {
        wideMap.insert(new Key(1, "name1"), "1-name1");
        wideMap.insert(new Key(1, "name2"), "1-name2");
        wideMap.insert(new Key(2, "name3"), "2-name3");
        wideMap.insert(new Key(3, "name4"), "3-name4");
        wideMap.insert(new Key(3, "name5"), "3-name5");

        KeyValueIterator<Key, String> iter = wideMap.iterator( //
                new Key(2, "name3"), //
                new Key(3, "name5"), //
                5);

        assertThat(iter.hasNext());
        KeyValue<Key, String> keyValue1 = iter.next();
        KeyValue<Key, String> keyValue2 = iter.next();
        KeyValue<Key, String> keyValue3 = iter.next();

        assertThat(keyValue1.getKey().getCount()).isEqualTo(2);
        assertThat(keyValue1.getKey().getName()).isEqualTo("name3");
        assertThat(keyValue1.getValue()).isEqualTo("2-name3");

        assertThat(keyValue2.getKey().getCount()).isEqualTo(3);
        assertThat(keyValue2.getKey().getName()).isEqualTo("name4");
        assertThat(keyValue2.getValue()).isEqualTo("3-name4");

        assertThat(keyValue3.getKey().getCount()).isEqualTo(3);
        assertThat(keyValue3.getKey().getName()).isEqualTo("name5");
        assertThat(keyValue3.getValue()).isEqualTo("3-name5");
    }

    @Test
    public void should_iterate_desc_exclusive_start_inclusive_end_with_count() throws Exception
    {
        wideMap.insert(new Key(1, "name1"), "1-name1");
        wideMap.insert(new Key(1, "name2"), "1-name2");
        wideMap.insert(new Key(2, "name3"), "2-name3");
        wideMap.insert(new Key(3, "name4"), "3-name4");
        wideMap.insert(new Key(3, "name5"), "3-name5");

        KeyValueIterator<Key, String> iter = //
        wideMap.iterator(new Key(3, "name5"), new Key(1, "name1"), 2,
                BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

        assertThat(iter.hasNext());

        KeyValue<Key, String> keyValue1 = iter.next();
        KeyValue<Key, String> keyValue2 = iter.next();

        assertThat(keyValue1.getKey().getCount()).isEqualTo(3);
        assertThat(keyValue1.getKey().getName()).isEqualTo("name4");
        assertThat(keyValue1.getValue()).isEqualTo("3-name4");

        assertThat(keyValue2.getKey().getCount()).isEqualTo(2);
        assertThat(keyValue2.getKey().getName()).isEqualTo("name3");
        assertThat(keyValue2.getValue()).isEqualTo("2-name3");

    }

    @Test
    public void should_remove() throws Exception
    {
        wideMap.insert(new Key(1, "name1"), "1-name1");
        wideMap.insert(new Key(1, "name2"), "1-name2");
        wideMap.insert(new Key(2, "name3"), "2-name3");
        wideMap.insert(new Key(3, "name4"), "3-name4");
        wideMap.insert(new Key(3, "name5"), "3-name5");

        wideMap.remove(new Key(1, "name2"));

        Composite startComp = new Composite();
        startComp.addComponent(0, 1, ComponentEquality.EQUAL);
        startComp.addComponent(1, "name2", ComponentEquality.EQUAL);

        Composite endComp = new Composite();
        endComp.addComponent(0, 1, ComponentEquality.EQUAL);
        endComp.addComponent(1, "name2", ComponentEquality.GREATER_THAN_EQUAL);

        List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(),
                startComp, endComp, false, 20);

        assertThat(columns).hasSize(0);
    }

    @Test
    public void should_remove_inclusive_start_exclusive_end() throws Exception
    {
        wideMap.insert(new Key(1, "name1"), "1-name1");
        wideMap.insert(new Key(1, "name2"), "1-name2");
        wideMap.insert(new Key(2, "name3"), "2-name3");
        wideMap.insert(new Key(3, "name4"), "3-name4");
        wideMap.insert(new Key(3, "name5"), "3-name5");

        wideMap.remove(new Key(1, "name2"), new Key(3, "name4"),
                BoundingMode.INCLUSIVE_START_BOUND_ONLY);

        List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(), null,
                null, false, 20);

        assertThat(columns).hasSize(3);
        assertThat(columns.get(0).right).isEqualTo("1-name1");
        assertThat(columns.get(1).right).isEqualTo("3-name4");
        assertThat(columns.get(2).right).isEqualTo("3-name5");
    }

    private Composite buildComposite(int key, ComponentEquality equality)
    {
        Composite startComp = new Composite();
        startComp.addComponent(0, key, equality);
        return startComp;
    }
}
