package fr.doan.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class ListPropertyMetaTest
{

	@Test
	public void should_exception_when_cannot_instanciate() throws Exception
	{
		ListPropertyMeta<String> listMeta = new ListPropertyMeta<String>();
		List<String> list = listMeta.newListInstance();

		assertThat(list).isInstanceOf(ArrayList.class);
	}
}
