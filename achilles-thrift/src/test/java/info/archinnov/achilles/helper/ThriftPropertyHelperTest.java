package info.archinnov.achilles.helper;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.type.WideMap.BoundingMode;
import info.archinnov.achilles.type.WideMap.OrderingMode;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import mapping.entity.TweetMultiKey;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Maps;

/**
 * ThriftPropertyHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftPropertyHelperTest
{

	@InjectMocks
	private ThriftPropertyHelper helper;

	@Mock
	private PropertyMeta<TweetMultiKey, String> multiKeyWideMeta;

	@Mock
	private MultiKeyProperties multiKeyProperties;

	@Test
	public void should_determine_composite_type_alias_for_column_family_check() throws Exception
	{
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
		propertyMeta.setType(PropertyType.WIDE_MAP);
		propertyMeta.setKeyClass(Integer.class);
		Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
		propertyMap.put("map", propertyMeta);
		entityMeta.setPropertyMetas(propertyMap);

		String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, false);

		assertThat(compatatorTypeAlias).isEqualTo(
				"CompositeType(org.apache.cassandra.db.marshal.BytesType)");
	}

	@Test
	public void should_determine_composite_type_alias_for_column_family() throws Exception
	{
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
		propertyMeta.setType(PropertyType.WIDE_MAP);
		propertyMeta.setKeyClass(Integer.class);
		Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
		propertyMap.put("map", propertyMeta);
		entityMeta.setPropertyMetas(propertyMap);

		String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, true);

		assertThat(compatatorTypeAlias).isEqualTo("(BytesType)");
	}

	@Test
	public void should_determine_composite_type_alias_for_multikey_column_family() throws Exception
	{
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta<TweetMultiKey, String> propertyMeta = new PropertyMeta<TweetMultiKey, String>();
		propertyMeta.setType(PropertyType.WIDE_MAP);
		propertyMeta.setKeyClass(TweetMultiKey.class);
		Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
		propertyMap.put("values", propertyMeta);
		entityMeta.setPropertyMetas(propertyMap);

		String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, true);

		assertThat(compatatorTypeAlias).isEqualTo("(UUIDType,UTF8Type,BytesType)");
	}

	@Test
	public void should_determine_composite_type_alias_for_multikey_column_family_check()
			throws Exception
	{
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta<TweetMultiKey, String> propertyMeta = new PropertyMeta<TweetMultiKey, String>();
		propertyMeta.setType(PropertyType.WIDE_MAP);
		propertyMeta.setKeyClass(TweetMultiKey.class);
		Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
		propertyMap.put("values", propertyMeta);
		entityMeta.setPropertyMetas(propertyMap);

		String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, false);

		assertThat(compatatorTypeAlias)
				.isEqualTo(
						"CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.BytesType)");
	}

	@Test
	public void should_build_multikey_for_composite() throws Exception
	{
		Method authorSetter = TweetMultiKey.class.getDeclaredMethod("setAuthor", String.class);
		Method idSetter = TweetMultiKey.class.getDeclaredMethod("setId", UUID.class);
		Method retweetCountSetter = TweetMultiKey.class.getDeclaredMethod("setRetweetCount",
				int.class);

		UUID uuid1 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		when(multiKeyWideMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);

		HColumn<Composite, String> hCol1 = buildHColumn(buildComposite("author1", uuid1, 11),
				"val1");

		when(multiKeyWideMeta.getKeyClass()).thenReturn(TweetMultiKey.class);

		when(multiKeyProperties.getComponentClasses()).thenReturn(
				Arrays.asList((Class<?>) String.class, UUID.class, Integer.class));
		when(multiKeyProperties.getComponentSetters()).thenReturn(
				Arrays.asList(authorSetter, idSetter, retweetCountSetter));

		TweetMultiKey multiKey = helper.buildMultiKeyFromComposite(multiKeyWideMeta, hCol1
				.getName()
				.getComponents());

		assertThat(multiKey.getAuthor()).isEqualTo("author1");
		assertThat(multiKey.getId()).isEqualTo(uuid1);
		assertThat(multiKey.getRetweetCount()).isEqualTo(11);
	}

	// Ascending order
	@Test
	public void should_return_determine_equalities_for_inclusive_start_and_end_asc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.INCLUSIVE_BOUNDS,
				OrderingMode.ASCENDING);
		assertThat(equality[0]).isEqualTo(EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_and_end_asc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.EXCLUSIVE_BOUNDS,
				OrderingMode.ASCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(LESS_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_inclusive_start_exclusive_end_asc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(
				BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.ASCENDING);
		assertThat(equality[0]).isEqualTo(EQUAL);
		assertThat(equality[1]).isEqualTo(LESS_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_inclusive_end_asc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(
				BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.ASCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	// Descending order
	@Test
	public void should_return_determine_equalities_for_inclusive_start_and_end_desc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.INCLUSIVE_BOUNDS,
				OrderingMode.DESCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_and_end_desc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.EXCLUSIVE_BOUNDS,
				OrderingMode.DESCENDING);
		assertThat(equality[0]).isEqualTo(LESS_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_inclusive_start_exclusive_end_desc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(
				BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.DESCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_inclusive_end_desc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(
				BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);
		assertThat(equality[0]).isEqualTo(LESS_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(EQUAL);
	}

	private Composite buildComposite(String author, UUID uuid, int retweetCount)
	{
		Composite composite = new Composite();
		composite.setComponent(0, author, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, uuid, UUID_SRZ, UUID_SRZ.getComparatorType().getTypeName());
		composite.setComponent(2, retweetCount, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());

		return composite;
	}

	private HColumn<Composite, String> buildHColumn(Composite comp, String value)
	{
		HColumn<Composite, String> hColumn = new HColumnImpl<Composite, String>(COMPOSITE_SRZ,
				STRING_SRZ);

		hColumn.setName(comp);
		hColumn.setValue(value);
		return hColumn;
	}
}
