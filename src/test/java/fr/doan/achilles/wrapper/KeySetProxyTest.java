package fr.doan.achilles.wrapper;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KeySetProxyTest
{

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add() throws Exception
	{
		KeySetProxy<Integer> proxy = new KeySetProxy<Integer>(new HashSet<Integer>());

		proxy.add(5);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add_all() throws Exception
	{
		KeySetProxy<Integer> proxy = new KeySetProxy<Integer>(new HashSet<Integer>());

		proxy.addAll(Arrays.asList(5, 7));
	}

}
