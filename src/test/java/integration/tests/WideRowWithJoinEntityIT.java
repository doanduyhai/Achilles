package integration.tests;

import static info.archinnov.achilles.columnFamily.AchillesColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.common.ThriftCassandraDaoTest.getColumnFamilyDao;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.entity.type.Pair;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import info.archinnov.achilles.entity.type.WideMap.OrderingMode;
import integration.tests.entity.User;
import integration.tests.entity.UserTestBuilder;
import integration.tests.entity.WideRowBeanWithJoinEntity;

import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import net.sf.cglib.proxy.Factory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ColumnFamilyWithJoinEntityIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideRowWithJoinEntityIT
{

	private ThriftGenericWideRowDao dao = getColumnFamilyDao(
			normalizerAndValidateColumnFamilyName(WideRowBeanWithJoinEntity.class.getName()),
			Long.class, Long.class);

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

	private WideRowBeanWithJoinEntity bean;

	private WideMap<Integer, User> friends;

	private Long id = 452L;

	private Long userId1 = 11L, //
			userId2 = 12L, //
			userId3 = 13L, //
			userId4 = 14L, userId5 = 15L;

	private User user1, user2, user3, user4, user5;

	@Before
	public void setUp()
	{
		bean = em.find(WideRowBeanWithJoinEntity.class, id);
		friends = bean.getFriends();

		user1 = UserTestBuilder.user().id(userId1).firstname("fn1").lastname("ln1").buid();
		user2 = UserTestBuilder.user().id(userId2).firstname("fn2").lastname("ln2").buid();
		user3 = UserTestBuilder.user().id(userId3).firstname("fn3").lastname("ln3").buid();
		user4 = UserTestBuilder.user().id(userId4).firstname("fn4").lastname("ln4").buid();
		user5 = UserTestBuilder.user().id(userId5).firstname("fn5").lastname("ln5").buid();
	}

	@Test
	public void should_cascade_insert_join_user() throws Exception
	{

		friends.insert(1, user1);
		friends.insert(2, user2);

		Composite startComp = new Composite();
		startComp.addComponent(0, 1, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, 2, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, Long>> columns = dao.findColumnsRange(id, startComp, endComp, false,
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
	public void should_find_external_join_tweets() throws Exception
	{

		insert5Users();

		List<Long> savedUserIds = dao.findValuesRange(id, null, null, false, 20);

		assertThat(savedUserIds).hasSize(5);
		assertThat(savedUserIds).containsExactly(user1.getId(), user2.getId(), user3.getId(),
				user4.getId(), user5.getId());

		List<KeyValue<Integer, User>> foundFriendKeyValues = friends.findReverse(2, 1, 5);

		assertThat(foundFriendKeyValues).hasSize(2);

		User foundUser1 = foundFriendKeyValues.get(0).getValue();
		User foundUser2 = foundFriendKeyValues.get(1).getValue();

		assertThat(foundFriendKeyValues.get(0).getKey()).isEqualTo(2);
		assertThat(foundUser1.getId()).isEqualTo(user2.getId());
		assertThat(foundUser1.getFirstname()).isEqualTo(user2.getFirstname());
		assertThat(foundUser1.getLastname()).isEqualTo(user2.getLastname());

		assertThat(foundFriendKeyValues.get(1).getKey()).isEqualTo(1);
		assertThat(foundUser2.getId()).isEqualTo(user1.getId());
		assertThat(foundUser2.getFirstname()).isEqualTo(user1.getFirstname());
		assertThat(foundUser2.getLastname()).isEqualTo(user1.getLastname());

		List<User> foundFriendValues = friends.findReverseValues(2, 1, 5);

		assertThat(foundFriendValues.get(0).getId()).isEqualTo(user2.getId());
		assertThat(foundFriendValues.get(0).getFirstname()).isEqualTo(user2.getFirstname());
		assertThat(foundFriendValues.get(0).getLastname()).isEqualTo(user2.getLastname());

		assertThat(foundFriendValues.get(1).getId()).isEqualTo(user1.getId());
		assertThat(foundFriendValues.get(1).getFirstname()).isEqualTo(user1.getFirstname());
		assertThat(foundFriendValues.get(1).getLastname()).isEqualTo(user1.getLastname());

		List<Integer> foundFriendKeys = friends.findReverseKeys(2, 1, 5);

		assertThat(foundFriendKeys.get(0)).isEqualTo(2);
		assertThat(foundFriendKeys.get(1)).isEqualTo(1);
	}

	@Test
	public void should_remove_join_user_id_but_not_user_entities() throws Exception
	{

		insert5Users();

		friends.remove(2, 4, BoundingMode.INCLUSIVE_START_BOUND_ONLY);

		List<Long> savedFriendIds = dao.findValuesRange(id, null, null, false, 10);

		assertThat(savedFriendIds).hasSize(3);
		assertThat(savedFriendIds).containsExactly(user1.getId(), user4.getId(), user5.getId());

		List<KeyValue<Integer, User>> foundFriendKeyValues = friends.find(1, 5, 10);

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

		KeyValueIterator<Integer, User> iterator = friends.iterator(3, 1, 10,
				BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.DESCENDING);

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

		em.remove(bean);

		List<Long> saveFriendIds = dao.findValuesRange(id, null, null, false, 10);

		assertThat(saveFriendIds).hasSize(0);

	}

	@Test
	public void should_proxy_join_entity() throws Exception
	{
		friends.insert(2, user1);

		User userProxy = friends.get(2);
		assertThat(userProxy).isInstanceOf(Factory.class);

		userProxy = friends.findFirst().getValue();
		assertThat(userProxy).isInstanceOf(Factory.class);

		userProxy = friends.findFirstValue();
		assertThat(userProxy).isInstanceOf(Factory.class);

		userProxy = friends.iterator(null, null, 1).next().getValue();
		assertThat(userProxy).isInstanceOf(Factory.class);

		userProxy = friends.iterator(null, null, 1).nextValue();
		assertThat(userProxy).isInstanceOf(Factory.class);
	}

	private void insert5Users()
	{
		friends.insert(1, user1);
		friends.insert(2, user2);
		friends.insert(3, user3);
		friends.insert(4, user4);
		friends.insert(5, user5);
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
