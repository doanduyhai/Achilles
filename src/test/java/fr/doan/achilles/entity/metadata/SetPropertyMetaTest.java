package fr.doan.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class SetPropertyMetaTest
{

	@Test
	public void should_exception_when_cannot_instanciate() throws Exception
	{

		SetPropertyMeta<String> setMeta = new SetPropertyMeta<String>();
		Set<String> set = setMeta.newSetInstance();

		assertThat(set).isInstanceOf(HashSet.class);
	}
}
