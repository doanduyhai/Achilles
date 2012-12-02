package fr.doan.achilles.entity.metadata.builder;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

import fr.doan.achilles.entity.metadata.WideMapMeta;
import fr.doan.achilles.entity.metadata.PropertyType;

/**
 * WideMapPropertyMetaBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapMetaBuilderTest
{

	@Test
	public void should_build() throws Exception
	{
		WideMapMeta<Integer, String> propertyMeta = WideMapMetaBuilder
				.wideMapPropertyMetaBuiler(Integer.class, String.class).propertyName("name").build();

		assertThat(propertyMeta.propertyType()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(propertyMeta.get("test")).isInstanceOf(String.class);
		assertThat(propertyMeta.getKey(5)).isInstanceOf(Integer.class);
		assertThat(propertyMeta.isInternal()).isTrue();
	}
}
