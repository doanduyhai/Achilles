package info.archinnov.achilles.wrapper;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * KeySetWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeySetWrapperTest
{

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add() throws Exception
	{
		KeySetWrapper<Long, Integer> wrapper = new KeySetWrapper<Long, Integer>(
				new HashSet<Integer>());

		wrapper.add(5);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add_all() throws Exception
	{
		KeySetWrapper<Long, Integer> wrapper = new KeySetWrapper<Long, Integer>(
				new HashSet<Integer>());

		wrapper.addAll(Arrays.asList(5, 7));
	}

}
