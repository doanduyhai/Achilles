package fr.doan.achilles.wrapper.factory;

import static fr.doan.achilles.entity.metadata.PropertyType.LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.SET;
import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static fr.doan.achilles.serializer.Utils.UUID_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.CorrectMultiKey;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.proxy.EntityWrapperUtil;
import fr.doan.achilles.serializer.Utils;

/**
 * DynamicCompositeKeyFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class DynamicCompositeKeyFactoryTest
{
	@InjectMocks
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private CompositeHelper helper;

	@Mock
	private EntityWrapperUtil util;

	@Mock
	private List<Method> componentGetters;

	@Test
	public void should_create_for_insert_simple_property() throws Exception
	{

		DynamicComposite comp = keyFactory.createForInsert("name", SIMPLE, 0);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(SIMPLE.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("name");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(0);
	}

	@Test
	public void should_create_for_insert_with_value() throws Exception
	{

		DynamicComposite comp = keyFactory.createForInsert("name", SIMPLE, "256", STRING_SRZ);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(SIMPLE.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("name");
		assertThat(comp.getComponent(2).getValue()).isEqualTo("256");
		assertThat(comp.getComponent(2).getSerializer()).isEqualTo(STRING_SRZ);
	}

	@Test
	public void should_create_for_insert_list_property() throws Exception
	{

		DynamicComposite comp = keyFactory.createForInsert("friends", LIST, 0);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(LIST.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(0);
	}

	@Test
	public void should_create_for_insert_set_property() throws Exception
	{

		DynamicComposite comp = keyFactory.createForInsert("followers", SET, 12345);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(SET.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("followers");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(12345);
	}

	@Test
	public void should_create_for_insert_map_property() throws Exception
	{

		DynamicComposite comp = keyFactory.createForInsert("preferences", MAP, -123933);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(MAP.flag());
		assertThat(comp.getComponent(1).getValue()).isEqualTo("preferences");
		assertThat(comp.getComponent(2).getValue()).isEqualTo(-123933);
	}

	@Test
	public void should_create_for_query_with_null_value() throws Exception
	{
		DynamicComposite comp = keyFactory.createForQuery("friends", PropertyType.LIST, null,
				LESS_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(2);
		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(LESS_THAN_EQUAL);
	}

	@Test
	public void should_create_for_query_without_value() throws Exception
	{
		DynamicComposite comp = keyFactory.createForQuery("friends", PropertyType.LIST,
				GREATER_THAN_EQUAL);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_create_for_query_with_value() throws Exception
	{
		List<Integer> list = Arrays.asList(1, 2, 3);
		DynamicComposite comp = keyFactory.createForQuery("friends", PropertyType.LIST, list,
				GREATER_THAN_EQUAL);

		assertThat(comp.getComponent(0).getValue()).isEqualTo(PropertyType.LIST.flag());
		assertThat(comp.getComponent(0).getEquality()).isSameAs(EQUAL);

		assertThat(comp.getComponent(1).getValue()).isEqualTo("friends");
		assertThat(comp.getComponent(1).getEquality()).isSameAs(EQUAL);

		assertThat(comp.getComponent(2).getValue()).isSameAs(list);
		assertThat(comp.getComponent(2).getEquality()).isSameAs(GREATER_THAN_EQUAL);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_for_multikey_insert() throws Exception
	{
		List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ,
				UUID_SRZ);
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		List<Object> keyValues = Arrays.asList((Object) 1, "a", uuid);

		DynamicComposite comp = keyFactory.createForInsertMultiKey("property", WIDE_MAP, keyValues,
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

		keyFactory.createForInsertMultiKey("property", WIDE_MAP, keyValues, serializers);
	}

	@SuppressWarnings("unchecked")
	@Test(expected = ValidationException.class)
	public void should_exception_when_values_size_contains_null() throws Exception
	{
		List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ,
				UUID_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1, "a", null);

		keyFactory.createForInsertMultiKey("property", WIDE_MAP, keyValues, serializers);
	}

	@Test
	public void should_create_for_multikey_query() throws Exception
	{
		List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
		serializers.add(INT_SRZ);
		serializers.add(STRING_SRZ);
		serializers.add(LONG_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1, "abc", 50L);

		when(helper.findLastNonNullIndexForComponents("property", keyValues)).thenReturn(2);

		DynamicComposite comp = keyFactory.createForQueryMultiKey("property", WIDE_MAP, keyValues,
				serializers, LESS_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(5);

		assertThat(comp.getComponents().get(4).getEquality()).isEqualTo(LESS_THAN_EQUAL);
	}

	@Test
	public void should_create_for_multikey_query_start_inclusive() throws Exception
	{
		List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
		serializers.add(Utils.INT_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1);

		DynamicComposite comp = keyFactory.createForQueryStart("property", WIDE_MAP, keyValues,
				serializers, true);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponents().get(2).getEquality()).isEqualTo(EQUAL);
	}

	@Test
	public void should_create_for_multikey_query_start_exclusive() throws Exception
	{
		List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
		serializers.add(Utils.INT_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1);

		DynamicComposite comp = keyFactory.createForQueryStart("property", WIDE_MAP, keyValues,
				serializers, false);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponents().get(2).getEquality()).isEqualTo(GREATER_THAN_EQUAL);

	}

	@Test
	public void should_create_for_multikey_query_end_inclusive() throws Exception
	{
		List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
		serializers.add(Utils.INT_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1);

		DynamicComposite comp = keyFactory.createForQueryEnd("property", WIDE_MAP, keyValues,
				serializers, true);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponents().get(2).getEquality()).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_create_for_multikey_query_end_exclusive() throws Exception
	{
		List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
		serializers.add(Utils.INT_SRZ);
		List<Object> keyValues = Arrays.asList((Object) 1);

		DynamicComposite comp = keyFactory.createForQueryEnd("property", WIDE_MAP, keyValues,
				serializers, false);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponents().get(2).getEquality()).isEqualTo(LESS_THAN_EQUAL);

	}

	@Test
	public void should_create_pair_for_query() throws Exception
	{
		boolean inclusiveStart = true, inclusiveEnd = true, reverse = false;
		when(helper.determineEquality(inclusiveStart, inclusiveEnd, reverse)).thenReturn(
				new ComponentEquality[]
				{
						EQUAL,
						GREATER_THAN_EQUAL
				});

		DynamicComposite[] composites = keyFactory.createForQuery("property", WIDE_MAP, 12L,
				inclusiveStart, 15L, inclusiveEnd, reverse);

		assertThat(composites).hasSize(2);

		assertThat(composites[0].getComponent(2).getValue()).isEqualTo(12L);
		assertThat(composites[0].getComponent(2).getEquality()).isEqualTo(EQUAL);

		assertThat(composites[1].getComponent(2).getValue()).isEqualTo(15L);
		assertThat(composites[1].getComponent(2).getEquality()).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_create_pair_for_multikey_query() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();
		List<Serializer<?>> componentSerializers = new ArrayList<Serializer<?>>();
		componentSerializers.add(STRING_SRZ);
		componentSerializers.add(INT_SRZ);
		componentSerializers.add(LONG_SRZ);

		boolean inclusiveStart = true, inclusiveEnd = true, reverse = false;
		when(helper.determineEquality(inclusiveStart, inclusiveEnd, reverse)).thenReturn(
				new ComponentEquality[]
				{
						EQUAL,
						GREATER_THAN_EQUAL
				});

		List<Object> startCompValues = Arrays.asList((Object) "test", 1, 2L);
		List<Object> endCompValues = Arrays.asList((Object) "toto", 3, 12L);

		when(util.determineMultiKey(start, componentGetters)).thenReturn(startCompValues);
		when(util.determineMultiKey(end, componentGetters)).thenReturn(endCompValues);
		when(helper.findLastNonNullIndexForComponents("property", startCompValues)).thenReturn(2);
		when(helper.findLastNonNullIndexForComponents("property", endCompValues)).thenReturn(2);

		DynamicComposite[] composites = keyFactory.createForMultiKeyQuery("property", //
				WIDE_MAP, //
				componentSerializers, //
				componentGetters, //
				start, //
				inclusiveStart, //
				end, //
				inclusiveEnd, //
				reverse);

		assertThat(composites).hasSize(2);

		assertThat(composites[0].getComponent(2).getValue()).isEqualTo("test");
		assertThat(composites[0].getComponent(2).getEquality()).isEqualTo(EQUAL);
		assertThat(composites[0].getComponent(3).getValue()).isEqualTo(1);
		assertThat(composites[0].getComponent(3).getEquality()).isEqualTo(EQUAL);
		assertThat(composites[0].getComponent(4).getValue()).isEqualTo(2L);
		assertThat(composites[0].getComponent(4).getEquality()).isEqualTo(EQUAL);

		assertThat(composites[1].getComponent(2).getValue()).isEqualTo("toto");
		assertThat(composites[1].getComponent(2).getEquality()).isEqualTo(EQUAL);
		assertThat(composites[1].getComponent(3).getValue()).isEqualTo(3);
		assertThat(composites[1].getComponent(3).getEquality()).isEqualTo(EQUAL);
		assertThat(composites[1].getComponent(4).getValue()).isEqualTo(12L);
		assertThat(composites[1].getComponent(4).getEquality()).isEqualTo(GREATER_THAN_EQUAL);
	}
}
