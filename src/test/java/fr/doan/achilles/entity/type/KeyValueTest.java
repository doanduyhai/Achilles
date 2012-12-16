package fr.doan.achilles.entity.type;

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
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.metadata.WideMapMeta;

/**
 * KeyValueTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeyValueTest
{
	@Mock
	private WideMapMeta<Integer, String> wideMapMeta;

	@Test
	public void should_create_from_hcolumn() throws Exception
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

		KeyValue<Integer, String> keyValue = new KeyValue<Integer, String>(hColumn, INT_SRZ,
				wideMapMeta);

		assertThat(keyValue.getKey()).isEqualTo(1);
		assertThat(keyValue.getValue()).isEqualTo("test");
		assertThat(keyValue.getTtl()).isEqualTo(12);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_from_list_of_hColumn() throws Exception
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

		List<KeyValue<Integer, String>> list = KeyValue.fromList(Arrays.asList(hColumn1, hColumn2),
				INT_SRZ, wideMapMeta);

		assertThat(list).hasSize(2);

		assertThat(list.get(0).getKey()).isEqualTo(1);
		assertThat(list.get(0).getValue()).isEqualTo("test1");
		assertThat(list.get(0).getTtl()).isEqualTo(12);

		assertThat(list.get(1).getKey()).isEqualTo(2);
		assertThat(list.get(1).getValue()).isEqualTo("test2");
		assertThat(list.get(1).getTtl()).isEqualTo(11);
	}
}
