package fr.doan.achilles.wrapper.factory;

import static fr.doan.achilles.entity.metadata.PropertyType.LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.SET;
import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.junit.Test;

import fr.doan.achilles.entity.metadata.PropertyType;

/**
 * DynamicCompositeKeyFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class DynamicCompositeKeyFactoryTest
{

	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	@Test
	public void should_build_composite_for_simple_property() throws Exception
	{

		DynamicComposite comp = keyFactory.buildForProperty("name", SIMPLE, 0);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(SIMPLE.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("name");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(0);
	}

	@Test
	public void should_build_composite_for_list_property() throws Exception
	{

		DynamicComposite comp = keyFactory.buildForProperty("friends", LIST, 0);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(LIST.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(0);
	}

	@Test
	public void should_build_composite_for_set_property() throws Exception
	{

		DynamicComposite comp = keyFactory.buildForProperty("followers", SET, 12345);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(SET.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("followers");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(12345);
	}

	@Test
	public void should_build_composite_for_map_property() throws Exception
	{

		DynamicComposite comp = keyFactory.buildForProperty("preferences", MAP, -123933);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(MAP.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("preferences");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(-123933);
	}

	@Test
	public void should_build_composite_comparator_start_inclusive() throws Exception
	{
		DynamicComposite comp = keyFactory.buildQueryComparatorStart("friends", PropertyType.LIST,
				1, false);

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
		DynamicComposite comp = keyFactory.buildQueryComparatorStart("friends", PropertyType.LIST,
				1, true);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(2).getValue()).isEqualTo(1);
		assertThat(comp.getComponent(2).getEquality()).isSameAs(
				ComponentEquality.GREATER_THAN_EQUAL);
	}

	@Test
	public void should_build_composite_comparator_end_inclusive() throws Exception
	{
		DynamicComposite comp = keyFactory.buildQueryComparatorEnd("friends", PropertyType.LIST, 4,
				false);

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
		DynamicComposite comp = keyFactory.buildQueryComparatorEnd("friends", PropertyType.LIST, 4,
				true);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(2).getValue()).isEqualTo(4);
		assertThat(comp.getComponent(2).getEquality()).isSameAs(ComponentEquality.LESS_THAN_EQUAL);
	}

	@Test
	public void should_build_composite_comparator() throws Exception
	{
		DynamicComposite comp = keyFactory.buildQueryComparator("friends", PropertyType.LIST,
				ComponentEquality.EQUAL);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

	}

	@Test
	public void should_validate_no_hole() throws Exception
	{
		List<Object> keyValues = Arrays.asList((Object) "a", "b", null, null);

		int lastNotNullIndex = keyFactory.validateNoHole("sdfsdf", keyValues);

		assertThat(lastNotNullIndex).isEqualTo(1);

	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_all_keys_null() throws Exception
	{
		List<Object> keyValues = Arrays.asList((Object) null, null, null);

		keyFactory.validateNoHole("sdfsdf", keyValues);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_hole() throws Exception
	{
		List<Object> keyValues = Arrays.asList((Object) "a", null, "b");

		keyFactory.validateNoHole("sdfsdf", keyValues);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_starting_with_hole() throws Exception
	{
		List<Object> keyValues = Arrays.asList((Object) null, "a", "b");

		keyFactory.validateNoHole("sdfsdf", keyValues);
	}
}
