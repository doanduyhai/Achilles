package fr.doan.achilles.wrapper.factory;

import static fr.doan.achilles.entity.metadata.PropertyType.LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.SET;
import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static fr.doan.achilles.serializer.Utils.UUID_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.junit.Test;

import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.serializer.Utils;

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
	public void should_build_query_comparator_with_null_value() throws Exception
	{
		DynamicComposite comp = keyFactory.buildQueryComparator("friends", PropertyType.LIST, null,
				LESS_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(2);
		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.LESS_THAN_EQUAL);
	}

	@Test
	public void should_build_query_comparator_start_inclusive() throws Exception
	{
		DynamicComposite comp = keyFactory.buildQueryComparatorStart("friends", PropertyType.LIST,
				1, true);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(2).getValue()).isEqualTo(1);
		assertThat(comp.getComponent(2).getEquality()).isSameAs(ComponentEquality.EQUAL);
	}

	@Test
	public void should_build_query_comparator_start_exlusive() throws Exception
	{
		DynamicComposite comp = keyFactory.buildQueryComparatorStart("friends", PropertyType.LIST,
				1, false);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(2).getValue()).isEqualTo(1);
		assertThat(comp.getComponent(2).getEquality()).isSameAs(
				ComponentEquality.GREATER_THAN_EQUAL);
	}

	@Test
	public void should_build_query_comparator_end_inclusive() throws Exception
	{
		DynamicComposite comp = keyFactory.buildQueryComparatorEnd("friends", PropertyType.LIST, 4,
				true);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(2).getValue()).isEqualTo(4);
		assertThat(comp.getComponent(2).getEquality()).isSameAs(
				ComponentEquality.GREATER_THAN_EQUAL);
	}

	@Test
	public void should_build_query_comparator_end_exlusive() throws Exception
	{
		DynamicComposite comp = keyFactory.buildQueryComparatorEnd("friends", PropertyType.LIST, 4,
				false);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(2).getValue()).isEqualTo(4);
		assertThat(comp.getComponent(2).getEquality()).isSameAs(ComponentEquality.LESS_THAN_EQUAL);
	}

	@Test
	public void should_build_query_comparator() throws Exception
	{
		DynamicComposite comp = keyFactory.buildQueryComparator("friends", PropertyType.LIST,
				ComponentEquality.EQUAL);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(ComponentEquality.EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(ComponentEquality.EQUAL);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_multikey_composite_for_property() throws Exception
	{
		List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ,
				UUID_SRZ);
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		List<Object> keyValues = Arrays.asList((Object) 1, "a", uuid);

		DynamicComposite comp = keyFactory.buildForProperty("property", WIDE_MAP, keyValues,
				serializers);

		assertThat(comp.getComponents()).hasSize(5);
		assertThat((byte[]) comp.getComponents().get(0).getValue()).isEqualTo(WIDE_MAP.flag());
		assertThat((String) comp.getComponents().get(1).getValue()).isEqualTo("property");
		assertThat((Integer) comp.getComponents().get(2).getValue()).isEqualTo(1);
		assertThat((String) comp.getComponents().get(3).getValue()).isEqualTo("a");
		assertThat((UUID) comp.getComponents().get(4).getValue()).isEqualTo(uuid);
	}

	@SuppressWarnings("unchecked")
	@Test(expected = ValidationException.class)
	public void should_exception_when_serializers_size_different_values_size() throws Exception
	{
		List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ,
				UUID_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1, "a");

		keyFactory.buildForProperty("property", WIDE_MAP, keyValues, serializers);
	}

	@SuppressWarnings("unchecked")
	@Test(expected = ValidationException.class)
	public void should_exception_when_values_size_contains_null() throws Exception
	{
		List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ,
				UUID_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1, "a", null);

		keyFactory.buildForProperty("property", WIDE_MAP, keyValues, serializers);
	}

	@Test
	public void should_build_multikey_query_comparator() throws Exception
	{
		List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
		serializers.add(Utils.INT_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1);

		DynamicComposite comp = keyFactory.buildQueryComparator("property", WIDE_MAP, keyValues,
				serializers, ComponentEquality.LESS_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(3);

		assertThat(comp.getComponents().get(2).getEquality()).isEqualTo(
				ComponentEquality.LESS_THAN_EQUAL);
	}

	@Test
	public void should_build_multikey_query_comparator_start_inclusive() throws Exception
	{
		List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
		serializers.add(Utils.INT_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1);

		DynamicComposite comp = keyFactory.buildQueryComparatorStart("property", WIDE_MAP,
				keyValues, serializers, true);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponents().get(2).getEquality()).isEqualTo(EQUAL);
	}

	@Test
	public void should_build_multikey_query_comparator_start_exclusive() throws Exception
	{
		List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
		serializers.add(Utils.INT_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1);

		DynamicComposite comp = keyFactory.buildQueryComparatorStart("property", WIDE_MAP,
				keyValues, serializers, false);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponents().get(2).getEquality()).isEqualTo(GREATER_THAN_EQUAL);

	}

	@Test
	public void should_build_multikey_query_comparator_end_inclusive() throws Exception
	{
		List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
		serializers.add(Utils.INT_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1);

		DynamicComposite comp = keyFactory.buildQueryComparatorEnd("property", WIDE_MAP, keyValues,
				serializers, true);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponents().get(2).getEquality()).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_build_multikey_query_comparator_end_exclusive() throws Exception
	{
		List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
		serializers.add(Utils.INT_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1);

		DynamicComposite comp = keyFactory.buildQueryComparatorEnd("property", WIDE_MAP, keyValues,
				serializers, false);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponents().get(2).getEquality()).isEqualTo(LESS_THAN_EQUAL);

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
