package fr.doan.achilles.holder.factory;

import static fr.doan.achilles.serializer.Utils.BYTE_SRZ;
import static fr.doan.achilles.serializer.Utils.COMPOSITE_SRZ;
import static fr.doan.achilles.serializer.Utils.DYNA_COMP_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.OBJECT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static fr.doan.achilles.serializer.Utils.UUID_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import mapping.entity.TweetMultiKey;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.serializer.Utils;

/**
 * KeyValueFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeyValueFactoryTest
{

	@InjectMocks
	private KeyValueFactory factory;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private PropertyMeta<TweetMultiKey, String> multiKeyWideMeta;

	@Test
	public void should_create() throws Exception
	{
		KeyValue<Integer, String> built = factory.create(15, "test");
		assertThat(built.getKey()).isEqualTo(15);
		assertThat(built.getValue()).isEqualTo("test");
	}

	@Test
	public void should_create_with_ttl() throws Exception
	{
		KeyValue<Integer, String> built = factory.create(15, "test", 14);
		assertThat(built.getKey()).isEqualTo(15);
		assertThat(built.getValue()).isEqualTo("test");
		assertThat(built.getTtl()).isEqualTo(14);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_from_dynamic_composite_hcolumn() throws Exception
	{
		HColumn<DynamicComposite, Object> hColumn = new HColumnImpl<DynamicComposite, Object>(
				DYNA_COMP_SRZ, OBJECT_SRZ);
		DynamicComposite dynComp = new DynamicComposite();
		dynComp.setComponent(0, 10, INT_SRZ);
		dynComp.setComponent(1, 10, INT_SRZ);
		dynComp.setComponent(2, 1, INT_SRZ);
		hColumn.setName(dynComp);
		hColumn.setValue("test");
		hColumn.setTtl(12);

		when(wideMapMeta.getKeySerializer()).thenReturn((Serializer) INT_SRZ);
		when(wideMapMeta.getValue("test")).thenReturn("test");
		when(wideMapMeta.isSingleKey()).thenReturn(true);

		KeyValue<Integer, String> keyValue = factory.createForWideMap(wideMapMeta, hColumn);

		assertThat(keyValue.getKey()).isEqualTo(1);
		assertThat(keyValue.getValue()).isEqualTo("test");
		assertThat(keyValue.getTtl()).isEqualTo(12);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_from_composite_column_list() throws Exception
	{
		HColumn<Composite, String> hColumn1 = new HColumnImpl<Composite, String>(COMPOSITE_SRZ,
				STRING_SRZ);
		Composite comp1 = new Composite();
		comp1.addComponent(0, 1, EQUAL);
		hColumn1.setName(comp1);
		hColumn1.setValue("test1");

		HColumn<Composite, String> hColumn2 = new HColumnImpl<Composite, String>(COMPOSITE_SRZ,
				STRING_SRZ);
		Composite comp2 = new Composite();
		comp2.addComponent(0, 2, EQUAL);
		hColumn2.setName(comp2);
		hColumn2.setValue("test2");

		HColumn<Composite, String> hColumn3 = new HColumnImpl<Composite, String>(COMPOSITE_SRZ,
				STRING_SRZ);
		Composite comp3 = new Composite();
		comp3.addComponent(0, 3, EQUAL);
		hColumn3.setName(comp3);
		hColumn3.setValue("test3");

		when(wideMapMeta.isSingleKey()).thenReturn(true);
		when(wideMapMeta.getKeySerializer()).thenReturn((Serializer) INT_SRZ);
		when(wideMapMeta.getValue("test1")).thenReturn("test1");
		when(wideMapMeta.getValue("test2")).thenReturn("test2");
		when(wideMapMeta.getValue("test3")).thenReturn("test3");

		List<KeyValue<Integer, String>> builtList = factory.createListForWideRow(//
				wideMapMeta, //
				Arrays.asList(hColumn1, hColumn2, hColumn3));

		assertThat(builtList).hasSize(3);

		assertThat(builtList.get(0).getKey()).isEqualTo(1);
		assertThat(builtList.get(0).getValue()).isEqualTo("test1");
		assertThat(builtList.get(0).getTtl()).isEqualTo(0);

		assertThat(builtList.get(1).getKey()).isEqualTo(2);
		assertThat(builtList.get(1).getValue()).isEqualTo("test2");
		assertThat(builtList.get(1).getTtl()).isEqualTo(0);

		assertThat(builtList.get(2).getKey()).isEqualTo(3);
		assertThat(builtList.get(2).getValue()).isEqualTo("test3");
		assertThat(builtList.get(2).getTtl()).isEqualTo(0);

	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_from_dynamic_composite_hcolumn_list() throws Exception
	{
		HColumn<DynamicComposite, Object> hColumn1 = new HColumnImpl<DynamicComposite, Object>(
				DYNA_COMP_SRZ, OBJECT_SRZ);
		DynamicComposite dynComp1 = new DynamicComposite();
		dynComp1.setComponent(0, 10, INT_SRZ);
		dynComp1.setComponent(1, 10, INT_SRZ);
		dynComp1.setComponent(2, 1, INT_SRZ);
		hColumn1.setName(dynComp1);
		hColumn1.setValue("test1");
		hColumn1.setTtl(12);

		HColumn<DynamicComposite, Object> hColumn2 = new HColumnImpl<DynamicComposite, Object>(
				DYNA_COMP_SRZ, OBJECT_SRZ);
		DynamicComposite dynComp2 = new DynamicComposite();
		dynComp2.setComponent(0, 10, INT_SRZ);
		dynComp2.setComponent(1, 10, INT_SRZ);
		dynComp2.setComponent(2, 2, INT_SRZ);
		hColumn2.setName(dynComp2);
		hColumn2.setValue("test2");
		hColumn2.setTtl(11);

		when(wideMapMeta.getValue("test1")).thenReturn("test1");
		when(wideMapMeta.getValue("test2")).thenReturn("test2");
		when(wideMapMeta.getKeySerializer()).thenReturn((Serializer) INT_SRZ);
		when(wideMapMeta.isSingleKey()).thenReturn(true);

		List<KeyValue<Integer, String>> list = factory.createListForWideMap(wideMapMeta,
				Arrays.asList(hColumn1, hColumn2));

		assertThat(list).hasSize(2);

		assertThat(list.get(0).getKey()).isEqualTo(1);
		assertThat(list.get(0).getValue()).isEqualTo("test1");
		assertThat(list.get(0).getTtl()).isEqualTo(12);

		assertThat(list.get(1).getKey()).isEqualTo(2);
		assertThat(list.get(1).getValue()).isEqualTo("test2");
		assertThat(list.get(1).getTtl()).isEqualTo(11);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_multikey_from_composite_hcolumn_list() throws Exception
	{
		Method authorSetter = TweetMultiKey.class.getDeclaredMethod("setAuthor", String.class);
		Method idSetter = TweetMultiKey.class.getDeclaredMethod("setId", UUID.class);
		Method retweetCountSetter = TweetMultiKey.class.getDeclaredMethod("setRetweetCount",
				int.class);

		UUID uuid1 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		UUID uuid2 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		UUID uuid3 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		HColumn<Composite, String> hCol1 = buildHColumn(buildComposite("author1", uuid1, 11),
				"val1");
		HColumn<Composite, String> hCol2 = buildHColumn(buildComposite("author2", uuid2, 12),
				"val2");
		HColumn<Composite, String> hCol3 = buildHColumn(buildComposite("author3", uuid3, 13),
				"val3");

		when(multiKeyWideMeta.getKeyClass()).thenReturn(TweetMultiKey.class);

		when(multiKeyWideMeta.getComponentSerializers()).thenReturn(
				Arrays.asList((Serializer<?>) STRING_SRZ, UUID_SRZ, INT_SRZ));
		when(multiKeyWideMeta.getComponentSetters()).thenReturn(
				Arrays.asList(authorSetter, idSetter, retweetCountSetter));

		when(multiKeyWideMeta.getValue("val1")).thenReturn("val1");
		when(multiKeyWideMeta.getValue("val2")).thenReturn("val2");
		when(multiKeyWideMeta.getValue("val3")).thenReturn("val3");

		List<KeyValue<TweetMultiKey, String>> multiKeys = factory.createListForWideRow(
				multiKeyWideMeta, Arrays.asList(hCol1, hCol2, hCol3));

		assertThat(multiKeys).hasSize(3);

		assertThat(multiKeys.get(0).getKey().getAuthor()).isEqualTo("author1");
		assertThat(multiKeys.get(0).getKey().getId()).isEqualTo(uuid1);
		assertThat(multiKeys.get(0).getKey().getRetweetCount()).isEqualTo(11);
		assertThat(multiKeys.get(0).getValue()).isEqualTo("val1");

		assertThat(multiKeys.get(1).getKey().getAuthor()).isEqualTo("author2");
		assertThat(multiKeys.get(1).getKey().getId()).isEqualTo(uuid2);
		assertThat(multiKeys.get(1).getKey().getRetweetCount()).isEqualTo(12);
		assertThat(multiKeys.get(1).getValue()).isEqualTo("val2");

		assertThat(multiKeys.get(2).getKey().getAuthor()).isEqualTo("author3");
		assertThat(multiKeys.get(2).getKey().getId()).isEqualTo(uuid3);
		assertThat(multiKeys.get(2).getKey().getRetweetCount()).isEqualTo(13);
		assertThat(multiKeys.get(2).getValue()).isEqualTo("val3");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_multikey_from_dynamic_composite_hcolumn_list() throws Exception
	{
		Method authorSetter = TweetMultiKey.class.getDeclaredMethod("setAuthor", String.class);
		Method idSetter = TweetMultiKey.class.getDeclaredMethod("setId", UUID.class);
		Method retweetCountSetter = TweetMultiKey.class.getDeclaredMethod("setRetweetCount",
				int.class);

		UUID uuid1 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		UUID uuid2 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		UUID uuid3 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		HColumn<DynamicComposite, Object> hCol1 = buildDynamicHColumn(
				buildDynamicComposite("author1", uuid1, 11), "val1");
		HColumn<DynamicComposite, Object> hCol2 = buildDynamicHColumn(
				buildDynamicComposite("author2", uuid2, 12), "val2");
		HColumn<DynamicComposite, Object> hCol3 = buildDynamicHColumn(
				buildDynamicComposite("author3", uuid3, 13), "val3");

		when(multiKeyWideMeta.getKeyClass()).thenReturn(TweetMultiKey.class);

		when(multiKeyWideMeta.getComponentSerializers()).thenReturn(
				Arrays.asList((Serializer<?>) STRING_SRZ, UUID_SRZ, INT_SRZ));
		when(multiKeyWideMeta.getComponentSetters()).thenReturn(
				Arrays.asList(authorSetter, idSetter, retweetCountSetter));

		when(multiKeyWideMeta.getValue("val1")).thenReturn("val1");
		when(multiKeyWideMeta.getValue("val2")).thenReturn("val2");
		when(multiKeyWideMeta.getValue("val3")).thenReturn("val3");

		List<KeyValue<TweetMultiKey, String>> multiKeys = factory.createListForWideMap(
				multiKeyWideMeta, Arrays.asList(hCol1, hCol2, hCol3));

		assertThat(multiKeys).hasSize(3);

		assertThat(multiKeys.get(0).getKey().getAuthor()).isEqualTo("author1");
		assertThat(multiKeys.get(0).getKey().getId()).isEqualTo(uuid1);
		assertThat(multiKeys.get(0).getKey().getRetweetCount()).isEqualTo(11);
		assertThat(multiKeys.get(0).getValue()).isEqualTo("val1");

		assertThat(multiKeys.get(1).getKey().getAuthor()).isEqualTo("author2");
		assertThat(multiKeys.get(1).getKey().getId()).isEqualTo(uuid2);
		assertThat(multiKeys.get(1).getKey().getRetweetCount()).isEqualTo(12);
		assertThat(multiKeys.get(1).getValue()).isEqualTo("val2");

		assertThat(multiKeys.get(2).getKey().getAuthor()).isEqualTo("author3");
		assertThat(multiKeys.get(2).getKey().getId()).isEqualTo(uuid3);
		assertThat(multiKeys.get(2).getKey().getRetweetCount()).isEqualTo(13);
		assertThat(multiKeys.get(2).getValue()).isEqualTo("val3");
	}

	private HColumn<Composite, String> buildHColumn(Composite comp, String value)
	{
		HColumn<Composite, String> hColumn = new HColumnImpl<Composite, String>(COMPOSITE_SRZ,
				STRING_SRZ);

		hColumn.setName(comp);
		hColumn.setValue(value);
		return hColumn;
	}

	private HColumn<DynamicComposite, Object> buildDynamicHColumn(DynamicComposite comp,
			String value)
	{
		HColumn<DynamicComposite, Object> hColumn = new HColumnImpl<DynamicComposite, Object>(
				Utils.DYNA_COMP_SRZ, Utils.OBJECT_SRZ);

		hColumn.setName(comp);
		hColumn.setValue(value);
		return hColumn;
	}

	private Composite buildComposite(String author, UUID uuid, int retweetCount)
	{
		Composite composite = new Composite();
		composite.setComponent(0, author, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, uuid, UUID_SRZ, UUID_SRZ.getComparatorType().getTypeName());
		composite.setComponent(2, retweetCount, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());

		return composite;
	}

	private DynamicComposite buildDynamicComposite(String author, UUID uuid, int retweetCount)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.setComponent(0, PropertyType.WIDE_MAP.flag(), BYTE_SRZ, BYTE_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(1, "multiKey1", STRING_SRZ, STRING_SRZ.getComparatorType()
				.getTypeName());
		composite.setComponent(2, author, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		composite.setComponent(3, uuid, UUID_SRZ, UUID_SRZ.getComparatorType().getTypeName());
		composite.setComponent(4, retweetCount, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());

		return composite;
	}
}
