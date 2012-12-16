package fr.doan.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

/**
 * WideMapPropertyMetaTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapMetaTest
{
	private WideMapMeta<String, String> propertyMeta = new WideMapMeta<String, String>();

	@Test
	public void should_cast_key() throws Exception
	{
		propertyMeta.setKeyClass(String.class);

		Object test = "test";

		String key = propertyMeta.getKey(test);
		assertThat(key).isEqualTo("test");
	}

	@Test
	public void should_cast_value() throws Exception
	{
		propertyMeta.setValueClass(String.class);

		Object test = "test";

		String value = propertyMeta.getValue(test);
		assertThat(value).isEqualTo("test");
	}

	@Test
	public void should_not_be_join() throws Exception
	{
		boolean isInternal = propertyMeta.isJoinColumn();

		assertThat(isInternal).isFalse();
	}

	@Test
	public void should_is_single_key() throws Exception
	{
		boolean isSingleKey = propertyMeta.isSingleKey();

		assertThat(isSingleKey).isTrue();
	}
}
