package info.archinnov.achilles.proxy.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * AchillesSetWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesSetWrapperTest
{

	@Test
	public void should_get_target() throws Exception
	{
		Set<String> target = new HashSet<String>();

		AchillesSetWrapper<String> setWrapper = new AchillesSetWrapper<String>(target);
		assertThat(setWrapper.getTarget()).isSameAs(target);
	}

}
