package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.table.TableHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.Type.*;
import static info.archinnov.achilles.type.BoundingMode.EXCLUSIVE_BOUNDS;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.Type;
import info.archinnov.achilles.test.integration.entity.EntityWithWideMaps;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;
import java.util.Iterator;
import java.util.List;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * WideMapWithEnumCompoundKeyIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWithEnumCompoundKeyIT
{

    private ThriftGenericWideRowDao dao = ThriftCassandraDaoTest
            .getColumnFamilyDao(normalizerAndValidateColumnFamilyName("clustered_with_enum_compound"), Long.class,
                    String.class);

    private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

    private EntityWithWideMaps entity;

    private WideMap<Type, String> wideMapWithEnumKey;

    private Long primaryKey;

    @Before
    public void setUp()
    {
        primaryKey = RandomUtils.nextLong();
        entity = new EntityWithWideMaps(primaryKey);
        entity = em.merge(entity);
        wideMapWithEnumKey = entity.getWideMapWithEnumKey();
    }

    @Test
    public void should_insert_values() throws Exception
    {

        wideMapWithEnumKey.insert(AUDIO, "audio");
        wideMapWithEnumKey.insert(FILE, "file");
        wideMapWithEnumKey.insert(IMAGE, "image");

        Composite startComp = new Composite();
        startComp.addComponent(0, "AUDIO", ComponentEquality.EQUAL);

        Composite endComp = new Composite();
        endComp.addComponent(0, "IMAGE", ComponentEquality.GREATER_THAN_EQUAL);

        List<Pair<Composite, String>> columns = dao.findColumnsRange(entity.getId(), startComp,
                endComp, false, 20);

        assertThat(columns).hasSize(3);
        assertThat(columns.get(0).right).isEqualTo("audio");
        assertThat(columns.get(1).right).isEqualTo("file");
        assertThat(columns.get(2).right).isEqualTo("image");

    }

    @Test
    public void should_get_value_by_key() throws Exception
    {
        wideMapWithEnumKey.insert(AUDIO, "audio");
        wideMapWithEnumKey.insert(FILE, "file");
        wideMapWithEnumKey.insert(IMAGE, "image");

        assertThat(wideMapWithEnumKey.get(AUDIO)).isEqualTo("audio");
    }

    @Test
    public void should_find_by_range() throws Exception
    {
        wideMapWithEnumKey.insert(AUDIO, "audio");
        wideMapWithEnumKey.insert(FILE, "file");
        wideMapWithEnumKey.insert(IMAGE, "image");

        List<KeyValue<Type, String>> foundTweets = wideMapWithEnumKey.find(AUDIO, IMAGE, 10);

        assertThat(foundTweets).hasSize(3);

        assertThat(foundTweets.get(0).getKey()).isEqualTo(AUDIO);
        assertThat(foundTweets.get(0).getValue()).isEqualTo("audio");

        assertThat(foundTweets.get(1).getKey()).isEqualTo(FILE);
        assertThat(foundTweets.get(1).getValue()).isEqualTo("file");

        assertThat(foundTweets.get(2).getKey()).isEqualTo(IMAGE);
        assertThat(foundTweets.get(2).getValue()).isEqualTo("image");

    }

    @Test
    public void should_get_iterator() throws Exception
    {
        wideMapWithEnumKey.insert(AUDIO, "audio");
        wideMapWithEnumKey.insert(FILE, "file");
        wideMapWithEnumKey.insert(IMAGE, "image");

        Iterator<KeyValue<Type, String>> iter = wideMapWithEnumKey.iterator(null, null, 10);

        KeyValue<Type, String> keyValue = iter.next();
        assertThat(keyValue.getKey()).isEqualTo(AUDIO);
        assertThat(keyValue.getValue()).isEqualTo("audio");

        keyValue = iter.next();
        assertThat(keyValue.getKey()).isEqualTo(FILE);
        assertThat(keyValue.getValue()).isEqualTo("file");

        keyValue = iter.next();
        assertThat(keyValue.getKey()).isEqualTo(IMAGE);
        assertThat(keyValue.getValue()).isEqualTo("image");

        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void should_remove_value() throws Exception
    {
        wideMapWithEnumKey.insert(AUDIO, "audio");
        wideMapWithEnumKey.insert(FILE, "file");
        wideMapWithEnumKey.insert(IMAGE, "image");

        wideMapWithEnumKey.remove(AUDIO);

        List<KeyValue<Type, String>> foundTweets = wideMapWithEnumKey.find(null, null, 10);

        assertThat(foundTweets).hasSize(2);
        assertThat(foundTweets.get(0).getKey()).isEqualTo(FILE);
        assertThat(foundTweets.get(0).getValue()).isEqualTo("file");

        assertThat(foundTweets.get(1).getKey()).isEqualTo(IMAGE);
        assertThat(foundTweets.get(1).getValue()).isEqualTo("image");

    }

    @Test
    public void should_remove_values_range() throws Exception
    {
        wideMapWithEnumKey.insert(AUDIO, "audio");
        wideMapWithEnumKey.insert(FILE, "file");
        wideMapWithEnumKey.insert(IMAGE, "image");

        wideMapWithEnumKey.remove(AUDIO, IMAGE, EXCLUSIVE_BOUNDS);

        List<KeyValue<Type, String>> foundTweets = wideMapWithEnumKey.find(null, null, 10);

        assertThat(foundTweets).hasSize(2);

        assertThat(foundTweets.get(0).getKey()).isEqualTo(AUDIO);
        assertThat(foundTweets.get(0).getValue()).isEqualTo("audio");

        assertThat(foundTweets.get(1).getKey()).isEqualTo(IMAGE);
        assertThat(foundTweets.get(1).getValue()).isEqualTo("image");

    }

    @After
    public void tearDown()
    {
        dao.truncate();
    }
}
