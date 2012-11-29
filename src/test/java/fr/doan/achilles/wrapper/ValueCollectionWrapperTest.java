package fr.doan.achilles.wrapper;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ValueCollectionWrapperTest
{

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add() throws Exception
	{
		ValueCollectionWrapper<String> wrapper = new ValueCollectionWrapper<String>(Arrays.asList("a"));

		wrapper.add("");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add_all() throws Exception
	{
		ValueCollectionWrapper<String> wrapper = new ValueCollectionWrapper<String>(Arrays.asList("a"));

		wrapper.addAll(Arrays.asList("a", "b"));
	}
}
