package integration.tests;

import static info.archinnov.achilles.table.TableHelper.*;
import static integration.tests.entity.CompoundKeyWithEnum.Type.*;
import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;
import integration.tests.entity.CompoundKeyWithEnum;
import integration.tests.entity.WideRowEntityWithEnumCompoundKey;
import java.util.Iterator;
import java.util.List;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * WideRowWithEnumCompoundKeyIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideRowWithEnumCompoundKeyIT
{

    private ThriftGenericWideRowDao dao = ThriftCassandraDaoTest.getColumnFamilyDao(
            normalizerAndValidateColumnFamilyName(WideRowEntityWithEnumCompoundKey.class.getName()), Long.class,
            String.class);

    private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

    private WideRowEntityWithEnumCompoundKey bean;

    private WideMap<CompoundKeyWithEnum, String> map;

    private Long id = 452L;

    @Before
    public void setUp()
    {
        bean = em.find(WideRowEntityWithEnumCompoundKey.class, id);
        map = bean.getMap();
    }

    @Test
    public void should_insert_values() throws Exception
    {

        insert3Values();

        Composite startComp = new Composite();
        startComp.addComponent(0, 11L, ComponentEquality.EQUAL);

        Composite endComp = new Composite();
        endComp.addComponent(0, 13L, ComponentEquality.GREATER_THAN_EQUAL);

        List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(), startComp,
                endComp, false, 20);

        assertThat(columns).hasSize(3);
        assertThat(columns.get(0).right).isEqualTo("audio");
        assertThat(columns.get(1).right).isEqualTo("file");
        assertThat(columns.get(2).right).isEqualTo("image");

    }

    @Test
    public void should_get_value_by_key() throws Exception
    {
        insert3Values();

        assertThat(map.get(new CompoundKeyWithEnum(11L, AUDIO))).isEqualTo("audio");
    }

    @Test
    public void should_find_by_range() throws Exception
    {
        insert5Values();

        List<KeyValue<CompoundKeyWithEnum, String>> foundTweets = map.find(new CompoundKeyWithEnum(11L, AUDIO),
                new CompoundKeyWithEnum(15L, IMAGE), 10);

        assertThat(foundTweets).hasSize(5);

        assertThat(foundTweets.get(0).getKey().getType()).isEqualTo(AUDIO);
        assertThat(foundTweets.get(0).getValue()).isEqualTo("audio");

        assertThat(foundTweets.get(1).getKey().getType()).isEqualTo(FILE);
        assertThat(foundTweets.get(1).getValue()).isEqualTo("file");

        assertThat(foundTweets.get(2).getKey().getType()).isEqualTo(IMAGE);
        assertThat(foundTweets.get(2).getValue()).isEqualTo("image1");

        assertThat(foundTweets.get(3).getKey().getType()).isEqualTo(IMAGE);
        assertThat(foundTweets.get(3).getValue()).isEqualTo("image2");

        assertThat(foundTweets.get(4).getKey().getType()).isEqualTo(IMAGE);
        assertThat(foundTweets.get(4).getValue()).isEqualTo("image3");
    }

    @Test
    public void should_get_iterator() throws Exception
    {
        insert5Values();

        Iterator<KeyValue<CompoundKeyWithEnum, String>> iter = map.iterator(null, null, 10);

        KeyValue<CompoundKeyWithEnum, String> keyValue = iter.next();
        assertThat(keyValue.getKey().getType()).isEqualTo(AUDIO);
        assertThat(keyValue.getValue()).isEqualTo("audio");

        keyValue = iter.next();
        assertThat(keyValue.getKey().getType()).isEqualTo(FILE);
        assertThat(keyValue.getValue()).isEqualTo("file");

        keyValue = iter.next();
        assertThat(keyValue.getKey().getType()).isEqualTo(IMAGE);
        assertThat(keyValue.getValue()).isEqualTo("image1");

        keyValue = iter.next();
        assertThat(keyValue.getKey().getType()).isEqualTo(IMAGE);
        assertThat(keyValue.getValue()).isEqualTo("image2");

        keyValue = iter.next();
        assertThat(keyValue.getKey().getType()).isEqualTo(IMAGE);
        assertThat(keyValue.getValue()).isEqualTo("image3");

    }

    @Test
    public void should_remove_value() throws Exception
    {
        insert3Values();

        map.remove(new CompoundKeyWithEnum(11L, AUDIO));

        List<KeyValue<CompoundKeyWithEnum, String>> foundTweets = map.find(null, null, 10);

        assertThat(foundTweets).hasSize(2);
        assertThat(foundTweets.get(0).getKey().getType()).isEqualTo(FILE);
        assertThat(foundTweets.get(0).getValue()).isEqualTo("file");

        assertThat(foundTweets.get(1).getKey().getType()).isEqualTo(IMAGE);
        assertThat(foundTweets.get(1).getValue()).isEqualTo("image");
    }

    @Test
    public void should_remove_values_range() throws Exception
    {
        insert5Values();

        map.remove(new CompoundKeyWithEnum(12L, null), new CompoundKeyWithEnum(14L, null));

        List<KeyValue<CompoundKeyWithEnum, String>> foundTweets = map.find(null, null, 10);

        assertThat(foundTweets).hasSize(2);

        assertThat(foundTweets.get(0).getKey().getType()).isEqualTo(AUDIO);
        assertThat(foundTweets.get(0).getValue()).isEqualTo("audio");

        assertThat(foundTweets.get(1).getKey().getType()).isEqualTo(IMAGE);
        assertThat(foundTweets.get(1).getValue()).isEqualTo("image3");
    }

    private void insert3Values()
    {
        map.insert(new CompoundKeyWithEnum(11L, AUDIO), "audio");
        map.insert(new CompoundKeyWithEnum(12L, FILE), "file");
        map.insert(new CompoundKeyWithEnum(13L, IMAGE), "image");
    }

    private void insert5Values()
    {
        map.insert(new CompoundKeyWithEnum(11L, AUDIO), "audio");
        map.insert(new CompoundKeyWithEnum(12L, FILE), "file");
        map.insert(new CompoundKeyWithEnum(13L, IMAGE), "image1");
        map.insert(new CompoundKeyWithEnum(14L, IMAGE), "image2");
        map.insert(new CompoundKeyWithEnum(15L, IMAGE), "image3");
    }

    @After
    public void tearDown()
    {
        dao.truncate();
    }
}
