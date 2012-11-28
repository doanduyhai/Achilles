package fr.doan.achilles.wrapper;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ValueCollectionProxyTest
{

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add() throws Exception
	{
		ValueCollectionProxy<String> proxy = new ValueCollectionProxy<String>(Arrays.asList("a"));

		proxy.add("");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add_all() throws Exception
	{
		ValueCollectionProxy<String> proxy = new ValueCollectionProxy<String>(Arrays.asList("a"));

		proxy.addAll(Arrays.asList("a", "b"));
	}
}
