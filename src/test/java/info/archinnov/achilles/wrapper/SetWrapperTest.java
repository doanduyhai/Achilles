package info.archinnov.achilles.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;

import info.archinnov.achilles.wrapper.SetWrapper;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;


/**
 * SetWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
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
