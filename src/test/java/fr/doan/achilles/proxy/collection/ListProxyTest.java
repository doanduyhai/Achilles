package fr.doan.achilles.proxy.collection;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class ListProxyTest
{

	@Test
	public void should_testname() throws Exception
	{
		List<String> list = new ArrayList<String>();

		list.add(null);
		list.add("b");
		list.add("c");

		list.set(2, "x");
		list.add(3, "y");
		// list.add(7, "Z");
		assertThat(list).hasSize(3);

		assertThat(list.get(2)).isEqualTo("x");
		assertThat(list.get(0)).isNull();

	}
}
