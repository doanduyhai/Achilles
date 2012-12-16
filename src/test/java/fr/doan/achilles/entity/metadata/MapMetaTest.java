package fr.doan.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class MapMetaTest
{
	@Test
	public void should_exception_when_cannot_instanciate() throws Exception
	{

		MapMeta<Integer, String> mapMeta = new MapMeta<Integer, String>();
		Map<?, String> map = mapMeta.newMap();

		assertThat(map).isInstanceOf(HashMap.class);
	}
}
