package fr.doan.achilles.proxy.collection;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class SetProxyTest
{

	@Test
	public void should_get_target() throws Exception
	{
		Set<String> target = new HashSet<String>();

		SetProxy<String> setProxy = new SetProxy<String>(target);
		assertThat(setProxy.getTarget()).isSameAs(target);
	}
}
