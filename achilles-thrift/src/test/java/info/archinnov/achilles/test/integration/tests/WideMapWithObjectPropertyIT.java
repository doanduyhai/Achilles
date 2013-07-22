package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithObjectValue.Holder;
import info.archinnov.achilles.test.integration.entity.EntityWithWideMaps;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;
import java.util.Iterator;
import java.util.List;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * WideMapWithObjectPropertyIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWithObjectPropertyIT
{

    private ThriftGenericWideRowDao dao = ThriftCassandraDaoTest.getColumnFamilyDao(
            normalizerAndValidateColumnFamilyName("clustered_with_object_value"), Long.class, String.class);

    private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

    private ObjectMapper objectMapper = new ObjectMapper();

    private EntityWithWideMaps entity;

    private WideMap<String, Holder> objectWideMap;

    private Long primaryKey;

    @Before
    public void setUp()
    {
        primaryKey = RandomUtils.nextLong();
        entity = new EntityWithWideMaps(primaryKey);
        entity = em.merge(entity);
        objectWideMap = entity.getObjectWideMap();
    }

    @Test
    public void should_insert_values() throws Exception
    {

        insert3Values();

        Composite startComp = new Composite();
        startComp.addComponent(0, "name1", ComponentEquality.EQUAL);

        Composite endComp = new Composite();
        endComp.addComponent(0, "name3", ComponentEquality.GREATER_THAN_EQUAL);

        List<Pair<Composite, String>> columns = dao.findColumnsRange(entity.getId(), startComp,
                endComp, false, 20);

        assertThat(columns).hasSize(3);
        assertThat(readHolder(columns.get(0).right).getContent()).isEqualTo("value1");
        assertThat(readHolder(columns.get(1).right).getContent()).isEqualTo("value2");
        assertThat(readHolder(columns.get(2).right).getContent()).isEqualTo("value3");

    }

    @Test
    public void should_get_value_by_key() throws Exception
    {
        insert3Values();

        assertThat(objectWideMap.get("name1").getContent()).isEqualTo("value1");
    }

    @Test
    public void should_find_values_by_range() throws Exception
    {
        insert5Values();

        List<KeyValue<String, Holder>> foundTweets = objectWideMap.find("name1", "name5", 10);

        assertThat(foundTweets).hasSize(5);
        assertThat(foundTweets.get(0).getValue().getContent()).isEqualTo("value1");
        assertThat(foundTweets.get(1).getValue().getContent()).isEqualTo("value2");
        assertThat(foundTweets.get(2).getValue().getContent()).isEqualTo("value3");
        assertThat(foundTweets.get(3).getValue().getContent()).isEqualTo("value4");
        assertThat(foundTweets.get(4).getValue().getContent()).isEqualTo("value5");

    }

    @Test
    public void should_get_iterator() throws Exception
    {
        insert5Values();

        Iterator<KeyValue<String, Holder>> iter = objectWideMap.iterator(null, null, 10);

        assertThat(iter.next().getValue().getContent()).isEqualTo("value1");
        assertThat(iter.next().getValue().getContent()).isEqualTo("value2");
        assertThat(iter.next().getValue().getContent()).isEqualTo("value3");
        assertThat(iter.next().getValue().getContent()).isEqualTo("value4");
        assertThat(iter.next().getValue().getContent()).isEqualTo("value5");

    }

    @Test
    public void should_remove_value() throws Exception
    {
        insert3Values();

        objectWideMap.remove("name1");

        List<KeyValue<String, Holder>> foundTweets = objectWideMap.find(null, null, 10);

        assertThat(foundTweets).hasSize(2);
        assertThat(foundTweets.get(0).getValue().getContent()).isEqualTo("value2");
        assertThat(foundTweets.get(1).getValue().getContent()).isEqualTo("value3");
    }

    @Test
    public void should_remove_values_range() throws Exception
    {
        insert5Values();

        objectWideMap.remove("name2", "name4");

        List<KeyValue<String, Holder>> foundTweets = objectWideMap.find(null, null, 10);

        assertThat(foundTweets).hasSize(2);
        assertThat(foundTweets.get(0).getValue().getContent()).isEqualTo("value1");
        assertThat(foundTweets.get(1).getValue().getContent()).isEqualTo("value5");
    }

    private void insert3Values()
    {
        objectWideMap.insert("name1", new Holder("value1"));
        objectWideMap.insert("name2", new Holder("value2"));
        objectWideMap.insert("name3", new Holder("value3"));
    }

    private void insert5Values()
    {
        objectWideMap.insert("name1", new Holder("value1"));
        objectWideMap.insert("name2", new Holder("value2"));
        objectWideMap.insert("name3", new Holder("value3"));
        objectWideMap.insert("name4", new Holder("value4"));
        objectWideMap.insert("name5", new Holder("value5"));
    }

    public Holder readHolder(String value) throws Exception
    {
        return objectMapper.readValue(value, Holder.class);
    }

    @After
    public void tearDown()
    {
        dao.truncate();
    }
}
