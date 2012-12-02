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

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_getter_asked() throws Exception
	{
		propertyMeta.getGetter();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_setter_asked() throws Exception
	{
		propertyMeta.getSetter();
	}

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

		String value = propertyMeta.get(test);
		assertThat(value).isEqualTo("test");
	}

	@Test
	public void should_is_internal() throws Exception
	{
		boolean isInternal = propertyMeta.isInternal();

		assertThat(isInternal).isTrue();
	}

	@Test
	public void should_is_single_key() throws Exception
	{
		boolean isSingleKey = propertyMeta.isSingleKey();

		assertThat(isSingleKey).isTrue();
	}
}
