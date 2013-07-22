package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.ThriftCassandraDaoTest.getColumnFamilyDao;
import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_START_BOUND_ONLY;
import static info.archinnov.achilles.type.OrderingMode.DESCENDING;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.test.builders.UserTestBuilder;
import info.archinnov.achilles.test.integration.entity.EntityWithWideMaps;
import info.archinnov.achilles.test.integration.entity.User;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;
import java.util.List;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import net.sf.cglib.proxy.Factory;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * WideRowWithJoinEntityIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWithJoinEntityIT
{

    private ThriftGenericWideRowDao dao = getColumnFamilyDao(
            normalizerAndValidateColumnFamilyName("clustered_with_join_value"), Long.class, Long.class);

    private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

    private EntityWithWideMaps entity;

    private WideMap<String, User> joinWideMap;

    private Long primaryKey;

    private Long userId1 = 11L,
            userId2 = 12L,
            userId3 = 13L,
            userId4 = 14L,
            userId5 = 15L;

    private User user1, user2, user3, user4, user5;

    @Before
    public void setUp()
    {
        primaryKey = RandomUtils.nextLong();
        entity = new EntityWithWideMaps(primaryKey);
        entity = em.merge(entity);
        joinWideMap = entity.getJoinWideMap();

        user1 = UserTestBuilder.user().id(userId1).firstname("fn1").lastname("ln1").buid();
        user2 = UserTestBuilder.user().id(userId2).firstname("fn2").lastname("ln2").buid();
        user3 = UserTestBuilder.user().id(userId3).firstname("fn3").lastname("ln3").buid();
        user4 = UserTestBuilder.user().id(userId4).firstname("fn4").lastname("ln4").buid();
        user5 = UserTestBuilder.user().id(userId5).firstname("fn5").lastname("ln5").buid();
    }

    @Test
    public void should_cascade_insert_join_user() throws Exception
    {
        joinWideMap.insert("name1", user1);
        joinWideMap.insert("name2", user2);

        Composite startComp = new Composite();
        startComp.addComponent(0, "name1", ComponentEquality.EQUAL);

        Composite endComp = new Composite();
        endComp.addComponent(0, "name2", ComponentEquality.GREATER_THAN_EQUAL);

        List<Pair<Composite, Long>> columns = dao.findColumnsRange(primaryKey, startComp, endComp, false,
                20);

        assertThat(columns).hasSize(2);

        assertThat(columns.get(0).right).isEqualTo(user1.getId());
        assertThat(columns.get(1).right).isEqualTo(user2.getId());

        User foundUser1 = em.find(User.class, user1.getId());
        User foundUser2 = em.find(User.class, user2.getId());

        assertThat(foundUser1.getId()).isEqualTo(user1.getId());
        assertThat(foundUser1.getFirstname()).isEqualTo(user1.getFirstname());
        assertThat(foundUser1.getLastname()).isEqualTo(user1.getLastname());

        assertThat(foundUser2.getId()).isEqualTo(user2.getId());
        assertThat(foundUser2.getFirstname()).isEqualTo(user2.getFirstname());
        assertThat(foundUser2.getLastname()).isEqualTo(user2.getLastname());

    }

    @Test
    public void should_find_external_join_entities() throws Exception
    {
        insert5Users();

        List<Long> savedUserIds = dao.findValuesRange(primaryKey, null, null, false, 20);

        assertThat(savedUserIds).hasSize(5);
        assertThat(savedUserIds).containsExactly(user1.getId(), user2.getId(), user3.getId(),
                user4.getId(), user5.getId());

        List<KeyValue<String, User>> foundFriendKeyValues = joinWideMap.findReverse("name2", "name1", 5);

        assertThat(foundFriendKeyValues).hasSize(2);

        User foundUser1 = foundFriendKeyValues.get(0).getValue();
        User foundUser2 = foundFriendKeyValues.get(1).getValue();

        assertThat(foundFriendKeyValues.get(0).getKey()).isEqualTo("name2");
        assertThat(foundUser1.getId()).isEqualTo(user2.getId());
        assertThat(foundUser1.getFirstname()).isEqualTo(user2.getFirstname());
        assertThat(foundUser1.getLastname()).isEqualTo(user2.getLastname());

        assertThat(foundFriendKeyValues.get(1).getKey()).isEqualTo("name1");
        assertThat(foundUser2.getId()).isEqualTo(user1.getId());
        assertThat(foundUser2.getFirstname()).isEqualTo(user1.getFirstname());
        assertThat(foundUser2.getLastname()).isEqualTo(user1.getLastname());

        List<User> foundFriendValues = joinWideMap.findReverseValues("name2", "name1", 5);

        assertThat(foundFriendValues.get(0).getId()).isEqualTo(user2.getId());
        assertThat(foundFriendValues.get(0).getFirstname()).isEqualTo(user2.getFirstname());
        assertThat(foundFriendValues.get(0).getLastname()).isEqualTo(user2.getLastname());

        assertThat(foundFriendValues.get(1).getId()).isEqualTo(user1.getId());
        assertThat(foundFriendValues.get(1).getFirstname()).isEqualTo(user1.getFirstname());
        assertThat(foundFriendValues.get(1).getLastname()).isEqualTo(user1.getLastname());

        List<String> foundFriendKeys = joinWideMap.findReverseKeys("name2", "name1", 5);

        assertThat(foundFriendKeys.get(0)).isEqualTo("name2");
        assertThat(foundFriendKeys.get(1)).isEqualTo("name1");
    }

    @Test
    public void should_remove_join_id_but_not_join_entities() throws Exception
    {
        insert5Users();

        joinWideMap.remove("name2", "name4", INCLUSIVE_START_BOUND_ONLY);

        List<Long> savedFriendIds = dao.findValuesRange(primaryKey, null, null, false, 10);

        assertThat(savedFriendIds).hasSize(3);
        assertThat(savedFriendIds).containsExactly(user1.getId(), user4.getId(), user5.getId());

        List<KeyValue<String, User>> foundFriendKeyValues = joinWideMap.find("name1", "name5", 10);

        assertThat(foundFriendKeyValues).hasSize(3);

        User foundUser1 = foundFriendKeyValues.get(0).getValue();
        User foundUser2 = foundFriendKeyValues.get(1).getValue();
        User foundUser3 = foundFriendKeyValues.get(2).getValue();

        assertThat(foundUser1.getId()).isEqualTo(user1.getId());
        assertThat(foundUser1.getFirstname()).isEqualTo(user1.getFirstname());
        assertThat(foundUser1.getLastname()).isEqualTo(user1.getLastname());

        assertThat(foundUser2.getId()).isEqualTo(user4.getId());
        assertThat(foundUser2.getFirstname()).isEqualTo(user4.getFirstname());
        assertThat(foundUser2.getLastname()).isEqualTo(user4.getLastname());

        assertThat(foundUser3.getId()).isEqualTo(user5.getId());
        assertThat(foundUser3.getFirstname()).isEqualTo(user5.getFirstname());
        assertThat(foundUser3.getLastname()).isEqualTo(user5.getLastname());

        assertThat(em.find(User.class, user1.getId())).isNotNull();
        assertThat(em.find(User.class, user2.getId())).isNotNull();
        assertThat(em.find(User.class, user3.getId())).isNotNull();
        assertThat(em.find(User.class, user4.getId())).isNotNull();
        assertThat(em.find(User.class, user5.getId())).isNotNull();

    }

    @Test
    public void should_iterate_through_users() throws Exception
    {
        insert5Users();

        KeyValueIterator<String, User> iterator = joinWideMap.iterator("name3", "name1", 10,
                INCLUSIVE_START_BOUND_ONLY, DESCENDING);

        User foundUser1 = iterator.next().getValue();
        User foundUser2 = iterator.next().getValue();

        assertThat(foundUser1.getId()).isEqualTo(user3.getId());
        assertThat(foundUser1.getFirstname()).isEqualTo(user3.getFirstname());
        assertThat(foundUser1.getLastname()).isEqualTo(user3.getLastname());

        assertThat(foundUser2.getId()).isEqualTo(user2.getId());
        assertThat(foundUser2.getFirstname()).isEqualTo(user2.getFirstname());
        assertThat(foundUser2.getLastname()).isEqualTo(user2.getLastname());

    }

    @Test
    public void should_remove_all_values_when_entity_is_removed() throws Exception
    {
        insert5Users();

        em.remove(entity);

        List<Long> saveFriendIds = dao.findValuesRange(primaryKey, null, null, false, 10);

        assertThat(saveFriendIds).hasSize(0);

    }

    @Test
    public void should_proxy_join_entity() throws Exception
    {
        joinWideMap.insert("name2", user1);

        User userProxy = joinWideMap.get("name2");
        assertThat(userProxy).isInstanceOf(Factory.class);

        userProxy = joinWideMap.findFirst().getValue();
        assertThat(userProxy).isInstanceOf(Factory.class);

        userProxy = joinWideMap.findFirstValue();
        assertThat(userProxy).isInstanceOf(Factory.class);

        userProxy = joinWideMap.iterator(null, null, 1).next().getValue();
        assertThat(userProxy).isInstanceOf(Factory.class);

        userProxy = joinWideMap.iterator(null, null, 1).nextValue();
        assertThat(userProxy).isInstanceOf(Factory.class);
    }

    private void insert5Users()
    {
        joinWideMap.insert("name1", user1);
        joinWideMap.insert("name2", user2);
        joinWideMap.insert("name3", user3);
        joinWideMap.insert("name4", user4);
        joinWideMap.insert("name5", user5);
    }

    @After
    public void tearDown()
    {
        dao.truncate();
    }
}
