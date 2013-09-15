/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.junit.AchillesInternalThriftResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class DirtyCheckIT {
	@Rule
	public AchillesInternalThriftResource resource = new AchillesInternalThriftResource(
			Steps.AFTER_TEST, "CompleteBean");

	private ThriftEntityManager em = resource.getEm();

	private ThriftGenericEntityDao dao = resource
			.getEntityDao(
					normalizerAndValidateColumnFamilyName(CompleteBean.class
							.getName()), Long.class);

	private CompleteBean bean;

	@Before
	public void setUp() {
		bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai")
				.age(35L).addFriends("foo", "bar")
				.addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014").buid();

		bean = em.merge(bean);
	}

	@Test
	public void should_dirty_check_list_element_add() throws Exception {
		bean.getFriends().add("qux");

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(columns.get(2).right).isEqualTo("qux");
	}

	@Test
	public void should_dirty_check_list_element_add_at_index() throws Exception {
		bean.getFriends().add(1, "qux");

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(columns.get(1).right).isEqualTo("qux");
		assertThat(columns.get(2).right).isEqualTo("bar");
	}

	@Test
	public void should_dirty_check_list_element_add_all() throws Exception {
		bean.getFriends().addAll(Arrays.asList("qux", "baz"));

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(4);
		assertThat(columns.get(2).right).isEqualTo("qux");
		assertThat(columns.get(3).right).isEqualTo("baz");
	}

	@Test
	public void should_dirty_check_list_element_clear() throws Exception {
		bean.getFriends().clear();

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(0);
	}

	@Test
	public void should_dirty_check_list_element_remove_at_index()
			throws Exception {
		bean.getFriends().remove(0);

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).right).isEqualTo("bar");
	}

	@Test
	public void should_dirty_check_list_element_remove_element()
			throws Exception {
		bean.getFriends().remove("bar");

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).right).isEqualTo("foo");
	}

	@Test
	public void should_dirty_check_list_element_remove_all() throws Exception {
		bean.getFriends().removeAll(Arrays.asList("foo", "qux"));

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).right).isEqualTo("bar");
	}

	@Test
	public void should_dirty_check_list_element_retain_all() throws Exception {
		bean.getFriends().retainAll(Arrays.asList("foo", "qux"));

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).right).isEqualTo("foo");
	}

	@Test
	public void should_dirty_check_list_element_sub_list_remove()
			throws Exception {
		bean.getFriends().subList(0, 1).remove(0);

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).right).isEqualTo("bar");
	}

	@Test
	public void should_dirty_check_list_element_set() throws Exception {
		bean.getFriends().set(1, "qux");

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);
		assertThat(columns.get(1).right).isEqualTo("qux");
	}

	@Test
	public void should_dirty_check_list_element_iterator_remove()
			throws Exception {
		Iterator<String> iter = bean.getFriends().iterator();

		iter.next();
		iter.remove();

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).right).isEqualTo("bar");
	}

	@Test
	public void should_dirty_check_list_element_list_iterator_remove()
			throws Exception {
		Iterator<String> iter = bean.getFriends().listIterator();

		iter.next();
		iter.remove();

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).right).isEqualTo("bar");
	}

	@Test
	public void should_dirty_check_list_element_list_iterator_set()
			throws Exception {
		ListIterator<String> iter = bean.getFriends().listIterator();

		iter.next();
		iter.set("qux");

		em.merge(bean);

		Composite startComp = startCompForList();
		Composite endComp = endComptForList();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);
		assertThat(columns.get(0).right).isEqualTo("qux");
	}

	@Test
	public void should_dirty_check_map_put_element() throws Exception {
		bean.getPreferences().put(4, "test");

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(4);

		assertThat(columns.get(3).left.get(2, STRING_SRZ)).isEqualTo("4");
		assertThat(columns.get(3).right).isEqualTo("test");
	}

	@Test
	public void should_dirty_check_map_remove_key() throws Exception {
		bean.getPreferences().remove(1);

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);
		assertThat(columns.get(0).left.get(2, STRING_SRZ)).isEqualTo("2");
		assertThat(columns.get(0).right).isEqualTo("Paris");
		assertThat(columns.get(1).left.get(2, STRING_SRZ)).isEqualTo("3");
		assertThat(columns.get(1).right).isEqualTo("75014");
	}

	@Test
	public void should_dirty_check_map_put_all() throws Exception {
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(3, "75015");
		map.put(4, "test");
		bean.getPreferences().putAll(map);

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(4);

		assertThat(columns.get(2).left.get(2, STRING_SRZ)).isEqualTo("3");
		assertThat(columns.get(2).right).isEqualTo("75015");
		assertThat(columns.get(3).left.get(2, STRING_SRZ)).isEqualTo("4");
		assertThat(columns.get(3).right).isEqualTo("test");
	}

	@Test
	public void should_dirty_check_map_keyset_remove() throws Exception {
		bean.getPreferences().keySet().remove(1);

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);
		assertThat(columns.get(0).left.get(2, STRING_SRZ)).isEqualTo("2");
		assertThat(columns.get(0).right).isEqualTo("Paris");
		assertThat(columns.get(1).left.get(2, STRING_SRZ)).isEqualTo("3");
		assertThat(columns.get(1).right).isEqualTo("75014");
	}

	@Test
	public void should_dirty_check_map_keyset_remove_all() throws Exception {
		bean.getPreferences().keySet().removeAll(Arrays.asList(1, 2, 5));

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).left.get(2, STRING_SRZ)).isEqualTo("3");
		assertThat(columns.get(0).right).isEqualTo("75014");
	}

	@Test
	public void should_dirty_check_map_keyset_retain_all() throws Exception {
		bean.getPreferences().keySet().retainAll(Arrays.asList(1, 3));

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);
		assertThat(columns.get(0).left.get(2, STRING_SRZ)).isEqualTo("1");
		assertThat(columns.get(0).right).isEqualTo("FR");
		assertThat(columns.get(1).left.get(2, STRING_SRZ)).isEqualTo("3");
		assertThat(columns.get(1).right).isEqualTo("75014");
	}

	@Test
	public void should_dirty_check_map_keyset_iterator_remove()
			throws Exception {
		Iterator<Integer> iter = bean.getPreferences().keySet().iterator();

		iter.next();
		iter.remove();

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);
		assertThat(columns.get(0).left.get(2, STRING_SRZ)).isEqualTo("2");
		assertThat(columns.get(0).right).isEqualTo("Paris");
		assertThat(columns.get(1).left.get(2, STRING_SRZ)).isEqualTo("3");
		assertThat(columns.get(1).right).isEqualTo("75014");
	}

	@Test
	public void should_dirty_check_map_valueset_remove() throws Exception {
		bean.getPreferences().values().remove("FR");

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);
		assertThat(columns.get(0).left.get(2, STRING_SRZ)).isEqualTo("2");
		assertThat(columns.get(0).right).isEqualTo("Paris");
		assertThat(columns.get(1).left.get(2, STRING_SRZ)).isEqualTo("3");
		assertThat(columns.get(1).right).isEqualTo("75014");
	}

	@Test
	public void should_dirty_check_map_valueset_remove_all() throws Exception {
		bean.getPreferences().values()
				.removeAll(Arrays.asList("FR", "Paris", "test"));

		em.merge(bean);

		Composite startComp = startCompForMap();

		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).left.get(2, STRING_SRZ)).isEqualTo("3");
		assertThat(columns.get(0).right).isEqualTo("75014");
	}

	@Test
	public void should_dirty_check_map_valueset_retain_all() throws Exception {
		bean.getPreferences().values()
				.retainAll(Arrays.asList("FR", "Paris", "test"));

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);
		assertThat(columns.get(0).left.get(2, STRING_SRZ)).isEqualTo("1");
		assertThat(columns.get(0).right).isEqualTo("FR");
		assertThat(columns.get(1).left.get(2, STRING_SRZ)).isEqualTo("2");
		assertThat(columns.get(1).right).isEqualTo("Paris");
	}

	@Test
	public void should_dirty_check_map_valueset_iterator_remove()
			throws Exception {
		Iterator<String> iter = bean.getPreferences().values().iterator();

		iter.next();
		iter.remove();

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);
		assertThat(columns.get(0).left.get(2, STRING_SRZ)).isEqualTo("2");
		assertThat(columns.get(0).right).isEqualTo("Paris");
		assertThat(columns.get(1).left.get(2, STRING_SRZ)).isEqualTo("3");
		assertThat(columns.get(1).right).isEqualTo("75014");
	}

	@Test
	public void should_dirty_check_map_entrySet_remove_entry() throws Exception {

		Set<Entry<Integer, String>> entrySet = bean.getPreferences().entrySet();

		Entry<Integer, String> entry = entrySet.iterator().next();

		entrySet.remove(entry);

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(2);
		assertThat(columns.get(0).left.get(2, STRING_SRZ)).isEqualTo("2");
		assertThat(columns.get(0).right).isEqualTo("Paris");
		assertThat(columns.get(1).left.get(2, STRING_SRZ)).isEqualTo("3");
		assertThat(columns.get(1).right).isEqualTo("75014");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_dirty_check_map_entrySet_remove_all_entry()
			throws Exception {

		Set<Entry<Integer, String>> entrySet = bean.getPreferences().entrySet();

		Iterator<Entry<Integer, String>> iterator = entrySet.iterator();

		Entry<Integer, String> entry1 = iterator.next();
		Entry<Integer, String> entry2 = iterator.next();

		entrySet.removeAll(Arrays.asList(entry1, entry2));

		em.merge(bean);

		Composite startComp = startCompForMap();
		Composite endComp = endCompForMap();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).left.get(2, STRING_SRZ)).isEqualTo("3");
		assertThat(columns.get(0).right).isEqualTo("75014");
	}

	@Test
	public void should_dirty_check_simple_property() throws Exception {
		bean.setName("another_name");

		em.merge(bean);

		Composite compo = new Composite();
		compo.addComponent(0, PropertyType.SIMPLE.flag(), EQUAL);
		compo.addComponent(1, "name", EQUAL);
		compo.addComponent(2, "0", EQUAL);

		Object reloadedName = dao.getValue(bean.getId(), compo);

		assertThat(reloadedName).isEqualTo("another_name");
	}

	@Test
	public void should_dirty_check_lazy_simple_property() throws Exception {
		bean.setLabel("label");

		em.merge(bean);

		Composite compo = new Composite();
		compo.addComponent(0, PropertyType.LAZY_SIMPLE.flag(), EQUAL);
		compo.addComponent(1, "label", EQUAL);
		compo.addComponent(2, "0", EQUAL);

		Object reloadedLabel = dao.getValue(bean.getId(), compo);

		assertThat(reloadedLabel).isEqualTo("label");
	}

	@Test
	public void should_dirty_check_lazy_simple_property_after_loading()
			throws Exception {
		assertThat(bean.getLabel()).isNull();

		bean.setLabel("label");

		em.merge(bean);

		Composite compo = new Composite();
		compo.addComponent(0, PropertyType.LAZY_SIMPLE.flag(), EQUAL);
		compo.addComponent(1, "label", EQUAL);
		compo.addComponent(2, "0", EQUAL);

		Object reloadedLabel = dao.getValue(bean.getId(), compo);

		assertThat(reloadedLabel).isEqualTo("label");
	}

	@Test
	public void should_cascade_dirty_check_join_simple_property()
			throws Exception {
		Tweet welcomeTweet = TweetTestBuilder.tweet().randomId()
				.content("Welcome").buid();

		CompleteBean myBean = CompleteBeanTestBuilder.builder().randomId()
				.name("DuyHai").age(35L).addFriends("foo", "bar")
				.addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014").buid();
		myBean.setWelcomeTweet(welcomeTweet);

		myBean = em.merge(myBean);

		Tweet welcomeTweetFromBean = myBean.getWelcomeTweet();
		welcomeTweetFromBean.setContent("new_welcome_message");

		em.merge(myBean);

		Tweet persistedWelcomeTweet = em
				.find(Tweet.class, welcomeTweet.getId());

		assertThat(persistedWelcomeTweet).isNotNull();
		assertThat(persistedWelcomeTweet.getContent()).isEqualTo(
				"new_welcome_message");

	}

	private Composite startCompForList() {
		Composite startComp = new Composite();
		startComp.addComponent(0, PropertyType.LAZY_LIST.flag(), EQUAL);
		startComp.addComponent(1, "friends", EQUAL);
		startComp.addComponent(2, "0", EQUAL);
		return startComp;
	}

	private Composite endComptForList() {
		Composite endComp = new Composite();
		endComp.addComponent(0, PropertyType.LAZY_LIST.flag(), EQUAL);
		endComp.addComponent(1, "friends", EQUAL);
		endComp.addComponent(2, "5", GREATER_THAN_EQUAL);
		return endComp;
	}

	private Composite startCompForMap() {
		Composite startComp = new Composite();
		startComp.addComponent(0, PropertyType.MAP.flag(), EQUAL);
		startComp.addComponent(1, "preferences", EQUAL);
		startComp.addComponent(2, "0", EQUAL);
		return startComp;
	}

	private Composite endCompForMap() {
		Composite endComp = new Composite();
		endComp.addComponent(0, PropertyType.MAP.flag(), EQUAL);
		endComp.addComponent(1, "preferences", EQUAL);
		endComp.addComponent(2, "5", GREATER_THAN_EQUAL);
		return endComp;
	}
}
