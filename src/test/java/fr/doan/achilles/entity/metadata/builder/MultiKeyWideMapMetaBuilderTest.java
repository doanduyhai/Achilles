package fr.doan.achilles.entity.metadata.builder;

import static fr.doan.achilles.serializer.Utils.DATE_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import mapping.entity.CompleteBean;

import org.junit.Test;

import parser.entity.MyMultiKey;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.type.WideMap;

/**
 * InternalMultiKeyWideMapPropertyMetaBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWideMapMetaBuilderTest
{

	@SuppressWarnings("unchecked")
	@Test
	public void should_build() throws Exception
	{
		Method nameGetter = MyMultiKey.class.getDeclaredMethod("getName");
		Method rankGetter = MyMultiKey.class.getDeclaredMethod("getRank");
		Method creationDateGetter = MyMultiKey.class.getDeclaredMethod("getCreationDate");

		Method[] accessors = new Method[2];
		accessors[0] = CompleteBean.class.getDeclaredMethod("getTweets", (Class<?>[]) null);
		accessors[1] = CompleteBean.class.getDeclaredMethod("setTweets", WideMap.class);

		List<Method> keyGetters = new ArrayList<Method>();
		keyGetters.add(nameGetter);
		keyGetters.add(rankGetter);
		keyGetters.add(creationDateGetter);

		MultiKeyWideMapMeta<Integer, String> propertyMeta = (MultiKeyWideMapMeta<Integer, String>) MultiKeyWideMapMetaBuilder
				.multiKeyWideMapPropertyMetaBuiler(Integer.class, String.class)
				.keyClasses(Arrays.asList((Class<?>) String.class, //
						(Class<?>) Integer.class, //
						(Class<?>) Date.class)) //
				.keyGetters(keyGetters).propertyName("name").accessors(accessors).build();

		assertThat(propertyMeta.getKeyGetters()).isSameAs(keyGetters);

		assertThat(propertyMeta.getKeySerializers()).containsExactly(STRING_SRZ, INT_SRZ, DATE_SRZ);
		assertThat(propertyMeta.isInternal()).isTrue();
	}
}
