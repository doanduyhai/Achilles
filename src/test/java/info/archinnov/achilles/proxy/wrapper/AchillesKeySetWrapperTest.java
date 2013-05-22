package info.archinnov.achilles.proxy.wrapper;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesKeySetWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class AchillesKeySetWrapperTest
{

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add() throws Exception
	{
		AchillesKeySetWrapper<Integer> wrapper = new AchillesKeySetWrapper<Integer>(
				new HashSet<Integer>());

		wrapper.add(5);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add_all() throws Exception
	{
		AchillesKeySetWrapper<Integer> wrapper = new AchillesKeySetWrapper<Integer>(
				new HashSet<Integer>());

		wrapper.addAll(Arrays.asList(5, 7));
	}

}
