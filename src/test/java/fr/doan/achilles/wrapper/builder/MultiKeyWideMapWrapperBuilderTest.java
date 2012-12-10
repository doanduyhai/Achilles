package fr.doan.achilles.wrapper.builder;

import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Arrays;

import me.prettyprint.hector.api.Serializer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.CorrectMultiKey;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.wrapper.MultiKeyWideMapWrapper;

/**
 * MultiKeyWideMapWrapperBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class MultiKeyWideMapWrapperBuilderTest
{

	@Mock
	private GenericDao<Long> dao;

	@Mock
	private MultiKeyWideMapMeta<CorrectMultiKey, String> wideMapMeta;

	@SuppressWarnings("unchecked")
	@Test
	public void should_build() throws Exception
	{
		Method nameGetter = CorrectMultiKey.class.getDeclaredMethod("getName");
		Method rankGetter = CorrectMultiKey.class.getDeclaredMethod("getRank");

		Method nameSetter = CorrectMultiKey.class.getDeclaredMethod("setName", String.class);
		Method rankSetter = CorrectMultiKey.class.getDeclaredMethod("setRank", int.class);

		MultiKeyWideMapWrapper<Long, CorrectMultiKey, String> wrapper = MultiKeyWideMapWrapperBuilder
				.builder(12L, dao, wideMapMeta) //
				.componentGetters(Arrays.asList(nameGetter, rankGetter)) //
				.componentSetters(Arrays.asList(nameSetter, rankSetter)) //
				.componentSerializers(Arrays.asList((Serializer<?>) STRING_SRZ, INT_SRZ)).build();

		assertThat(wrapper).isNotNull();
	}
}
