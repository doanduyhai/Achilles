package fr.doan.achilles.entity.metadata.builder;

import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Method;

import mapping.entity.CompleteBean;

import org.junit.Test;

import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.metadata.WideMapMeta;
import fr.doan.achilles.entity.type.WideMap;

/**
 * WideMapPropertyMetaBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapMetaBuilderTest
{

	@SuppressWarnings("unchecked")
	@Test
	public void should_build() throws Exception
	{
		Method[] accessors = new Method[2];
		accessors[0] = CompleteBean.class.getDeclaredMethod("getTweets", (Class<?>[]) null);
		accessors[1] = CompleteBean.class.getDeclaredMethod("setTweets", WideMap.class);

		WideMapMeta<Integer, String> propertyMeta = (WideMapMeta<Integer, String>) WideMapMetaBuilder
				.wideMapPropertyMetaBuiler(Integer.class, String.class).propertyName("tweets")
				.accessors(accessors).build();

		assertThat(propertyMeta.propertyType()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(propertyMeta.get("test")).isInstanceOf(String.class);
		assertThat(propertyMeta.getKey(5)).isInstanceOf(Integer.class);
		assertThat(propertyMeta.isInternal()).isTrue();
	}
}
