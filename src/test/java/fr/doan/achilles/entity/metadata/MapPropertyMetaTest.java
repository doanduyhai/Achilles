package fr.doan.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class MapPropertyMetaTest
{
	@Test
	public void should_exception_when_cannot_instanciate() throws Exception
	{

		MapPropertyMeta<Integer, String> mapMeta = new MapPropertyMeta<Integer, String>();
		Map<?, String> map = mapMeta.newMapInstance();

		assertThat(map).isInstanceOf(HashMap.class);
	}
}
