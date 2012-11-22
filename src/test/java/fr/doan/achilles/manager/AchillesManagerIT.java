package fr.doan.achilles.manager;

import static fr.doan.achilles.common.CassandraDaoTest.getCluster;
import static fr.doan.achilles.common.CassandraDaoTest.getDao;
import static fr.doan.achilles.common.CassandraDaoTest.getKeyspace;
import static fr.doan.achilles.metadata.PropertyType.END_EAGER;
import static fr.doan.achilles.metadata.PropertyType.START_EAGER;
import static fr.doan.achilles.metadata.builder.EntityMetaBuilder.normalizeColumnFamilyName;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.List;

import mapping.entity.CompleteBean;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.junit.Test;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.factory.AchillesEntityManagerFactoryImpl;
import fr.doan.achilles.holder.KeyValueHolder;

public class AchillesManagerIT
{

	private final String ENTITY_PACKAGE = "mapping.entity";
	private GenericDao<Long> dao = getDao(LONG_SRZ, normalizeColumnFamilyName(CompleteBean.class.getCanonicalName()));

	private AchillesEntityManagerFactoryImpl factory = new AchillesEntityManagerFactoryImpl(getCluster(), getKeyspace(), ENTITY_PACKAGE, true);

	private AchillesEntityManager em = (AchillesEntityManager) factory.createEntityManager();

	@Test
	public void should_persist() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(1L).name("DuyHai").age(35L).addFriends("foo", "bar").addFollowers("George", "Paul")
				.addPreference(1, "FR").addPreference(2, "Paris").addPreference(3, "75014").buid();

		em.persist(bean);

		Composite startCompositeForEagerFetch = new Composite();
		startCompositeForEagerFetch.addComponent(0, START_EAGER.flag(), ComponentEquality.EQUAL);

		Composite endCompositeForEagerFetch = new Composite();
		endCompositeForEagerFetch.addComponent(0, END_EAGER.flag(), ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, Object>> columns = dao.findColumnsRange(1L, startCompositeForEagerFetch, endCompositeForEagerFetch, false, 20);

		assertThat(columns).hasSize(9);

		Pair<Composite, Object> age = columns.get(0);

		Pair<Composite, Object> name = columns.get(1);

		Pair<Composite, Object> foo = columns.get(2);
		Pair<Composite, Object> bar = columns.get(3);

		Pair<Composite, Object> George = columns.get(4);
		Pair<Composite, Object> Paul = columns.get(5);

		Pair<Composite, Object> FR = columns.get(6);
		Pair<Composite, Object> Paris = columns.get(7);
		Pair<Composite, Object> _75014 = columns.get(8);

		assertThat(age.left.get(1, STRING_SRZ)).isEqualTo("age_in_years");
		assertThat(age.right).isEqualTo(35L);

		assertThat(name.left.get(1, STRING_SRZ)).isEqualTo("name");
		assertThat(name.right).isEqualTo("DuyHai");

		assertThat(foo.left.get(1, STRING_SRZ)).isEqualTo("friends");
		assertThat(foo.right).isEqualTo("foo");
		assertThat(bar.left.get(1, STRING_SRZ)).isEqualTo("friends");
		assertThat(bar.right).isEqualTo("bar");

		assertThat(George.left.get(1, STRING_SRZ)).isEqualTo("followers");
		assertThat(George.right).isIn("George", "Paul");
		assertThat(Paul.left.get(1, STRING_SRZ)).isEqualTo("followers");
		assertThat(Paul.right).isIn("George", "Paul");

		assertThat(FR.left.get(1, STRING_SRZ)).isEqualTo("preferences");
		KeyValueHolder country = (KeyValueHolder) FR.right;
		assertThat(country.getKey()).isEqualTo(1);
		assertThat(country.getValue()).isEqualTo("FR");

		assertThat(Paris.left.get(1, STRING_SRZ)).isEqualTo("preferences");
		KeyValueHolder city = (KeyValueHolder) Paris.right;
		assertThat(city.getKey()).isEqualTo(2);
		assertThat(city.getValue()).isEqualTo("Paris");

		assertThat(_75014.left.get(1, STRING_SRZ)).isEqualTo("preferences");
		KeyValueHolder zipCode = (KeyValueHolder) _75014.right;
		assertThat(zipCode.getKey()).isEqualTo(3);
		assertThat(zipCode.getValue()).isEqualTo("75014");
	}

	@Test
	public void should_find() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(2L).name("Jonathan").age(40L).addFriends("bob")
				.addFollowers("Billy", "Stephen", "Jacky").addPreference(1, "US").addPreference(2, "New York").buid();

		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, 2L);

		assertThat(found).isNotNull();
	}
}
