package fr.doan.achilles.holder.factory;

import static fr.doan.achilles.serializer.Utils.DYNA_COMP_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.OBJECT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.metadata.WideMapMeta;
import fr.doan.achilles.holder.KeyValue;

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
	private WideMapMeta<Integer, String> wideMapMeta;

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

		when(wideMapMeta.getValue("test")).thenReturn("test");

		KeyValue<Integer, String> keyValue = factory.createFromDynamicCompositeColumn(hColumn,
				INT_SRZ, wideMapMeta);

		assertThat(keyValue.getKey()).isEqualTo(1);
		assertThat(keyValue.getValue()).isEqualTo("test");
		assertThat(keyValue.getTtl()).isEqualTo(12);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_from_column_list() throws Exception
	{
		HColumn<Integer, Object> hColumn1 = new HColumnImpl<Integer, Object>(INT_SRZ, OBJECT_SRZ);
		hColumn1.setName(1);
		hColumn1.setValue("test1");

		HColumn<Integer, Object> hColumn2 = new HColumnImpl<Integer, Object>(INT_SRZ, OBJECT_SRZ);
		hColumn2.setName(2);
		hColumn2.setValue("test2");

		HColumn<Integer, Object> hColumn3 = new HColumnImpl<Integer, Object>(INT_SRZ, OBJECT_SRZ);
		hColumn3.setName(3);
		hColumn3.setValue("test3");

		when(wideMapMeta.getValue("test1")).thenReturn("test1");
		when(wideMapMeta.getValue("test2")).thenReturn("test2");
		when(wideMapMeta.getValue("test3")).thenReturn("test3");

		List<KeyValue<Integer, String>> builtList = factory.createFromColumnList(//
				Arrays.asList(hColumn1, hColumn2, hColumn3), //
				INT_SRZ, //
				wideMapMeta);

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

	@SuppressWarnings("unchecked")
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

		List<KeyValue<Integer, String>> list = factory.createFromDynamicCompositeColumnList(
				Arrays.asList(hColumn1, hColumn2), INT_SRZ, wideMapMeta);

		assertThat(list).hasSize(2);

		assertThat(list.get(0).getKey()).isEqualTo(1);
		assertThat(list.get(0).getValue()).isEqualTo("test1");
		assertThat(list.get(0).getTtl()).isEqualTo(12);

		assertThat(list.get(1).getKey()).isEqualTo(2);
		assertThat(list.get(1).getValue()).isEqualTo("test2");
		assertThat(list.get(1).getTtl()).isEqualTo(11);
	}
}
