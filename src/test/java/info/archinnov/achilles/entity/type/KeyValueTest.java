package info.archinnov.achilles.entity.type;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

/**
 * KeyValueTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueTest
{
	@Test
	public void should_to_string() throws Exception
	{
		KeyValue<Integer, String> kv = new KeyValue<Integer, String>(11, "value", 1);

		assertThat(kv.toString()).isEqualTo("KeyValue [key=11, value=value, ttl=1]");
	}
}
