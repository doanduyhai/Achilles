package fr.doan.achilles.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import fr.doan.achilles.wrapper.SetProxy;

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
