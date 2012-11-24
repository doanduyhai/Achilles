package fr.doan.achilles.dao;

import static fr.doan.achilles.metadata.PropertyType.END_EAGER;
import static fr.doan.achilles.metadata.PropertyType.LIST;
import static fr.doan.achilles.metadata.PropertyType.MAP;
import static fr.doan.achilles.metadata.PropertyType.SET;
import static fr.doan.achilles.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.metadata.PropertyType.START_EAGER;
import static org.fest.assertions.api.Assertions.assertThat;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.serializer.Utils;

@RunWith(MockitoJUnitRunner.class)
public class GenericDaoTest
{

	@InjectMocks
	private final GenericDao<Long> dao = new GenericDao<Long>();

	@Mock
	private ExecutingKeyspace keyspace;

	@Mock
	private final Serializer<Long> serializer = Utils.LONG_SRZ;

	@Test
	public void should_build_mutator() throws Exception
	{

		Mutator<Long> mutator = dao.buildMutator();
		assertThat(mutator).isNotNull();
	}

	@Test
	public void should_build_composite_for_simple_property() throws Exception
	{

		Composite comp = dao.buildCompositeForProperty("name", SIMPLE, 0);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(SIMPLE.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("name");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(0);
	}

	@Test
	public void should_build_composite_for_list_property() throws Exception
	{

		Composite comp = dao.buildCompositeForProperty("friends", LIST, 0);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(LIST.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(0);
	}

	@Test
	public void should_build_composite_for_set_property() throws Exception
	{

		Composite comp = dao.buildCompositeForProperty("followers", SET, 12345);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(SET.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("followers");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(12345);
	}

	@Test
	public void should_build_composite_for_map_property() throws Exception
	{

		Composite comp = dao.buildCompositeForProperty("preferences", MAP, -123933);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(MAP.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("preferences");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(-123933);
	}

	@Test
	public void should_build_start_composite_for_eager_fetch() throws Exception
	{

		Composite comp = (Composite) ReflectionTestUtils.getField(dao, "startCompositeForEagerFetch");

		assertThat(comp.getComponent(0).getValue()).isEqualTo(START_EAGER.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);
	}

	@Test
	public void should_build_end_composite_for_eager_fetch() throws Exception
	{

		Composite comp = (Composite) ReflectionTestUtils.getField(dao, "endCompositeForEagerFetch");

		assertThat(comp.getComponent(0).getValue()).isEqualTo(END_EAGER.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.GREATER_THAN_EQUAL);
	}

	@Test
	public void should_build_composite_comparator_start_inclusive() throws Exception
	{
		Composite comp = dao.buildCompositeComparatorStart("friends", PropertyType.LIST, 1, false);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(2).getValue()).isEqualTo(1);
		assertThat(comp.getComponent(2).getEquality()).isSameAs(ComponentEquality.EQUAL);
	}

	@Test
	public void should_build_composite_comparator_start_exlusive() throws Exception
	{
		Composite comp = dao.buildCompositeComparatorStart("friends", PropertyType.LIST, 1, true);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(2).getValue()).isEqualTo(1);
		assertThat(comp.getComponent(2).getEquality()).isSameAs(ComponentEquality.GREATER_THAN_EQUAL);
	}

	@Test
	public void should_build_composite_comparator_end_inclusive() throws Exception
	{
		Composite comp = dao.buildCompositeComparatorEnd("friends", PropertyType.LIST, 4, false);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(2).getValue()).isEqualTo(4);
		assertThat(comp.getComponent(2).getEquality()).isSameAs(ComponentEquality.EQUAL);
	}

	@Test
	public void should_build_composite_comparator_end_exlusive() throws Exception
	{
		Composite comp = dao.buildCompositeComparatorEnd("friends", PropertyType.LIST, 4, true);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(2).getValue()).isEqualTo(4);
		assertThat(comp.getComponent(2).getEquality()).isSameAs(ComponentEquality.LESS_THAN_EQUAL);
	}

	@Test
	public void should_build_composite_comparator_start() throws Exception
	{
		Composite comp = dao.buildCompositeComparatorStart("friends", PropertyType.LIST);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

	}

	@Test
	public void should_build_composite_comparator_end() throws Exception
	{
		Composite comp = dao.buildCompositeComparatorEnd("friends", PropertyType.LIST);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.GREATER_THAN_EQUAL);
	}
}
