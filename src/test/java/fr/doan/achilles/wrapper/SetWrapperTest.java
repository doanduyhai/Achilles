package fr.doan.achilles.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import fr.doan.achilles.wrapper.SetWrapper;

public class SetWrapperTest
{

	@Test
	public void should_get_target() throws Exception
	{
		Set<String> target = new HashSet<String>();

		SetWrapper<String> setWrapper = new SetWrapper<String>(target);
		assertThat(setWrapper.getTarget()).isSameAs(target);
	}

}
