package info.archinnov.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

/**
 * ThriftJoinHColumnTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftJoinHColumnTest
{

	@Test
	public void should_test_all_methods() throws Exception
	{
		ThriftJoinHColumn<Integer, String> joinHCol = new ThriftJoinHColumn<Integer, String>();

		joinHCol.setName(11);
		joinHCol.setValue("val");
		joinHCol.setClock(1000);
		joinHCol.setTtl(10);

		assertThat(joinHCol.getName()).isEqualTo(11);
		assertThat(joinHCol.getValue()).isEqualTo("val");
		assertThat(joinHCol.getTtl()).isEqualTo(10);
		assertThat(joinHCol.getClock()).isEqualTo(0);

		assertThat(joinHCol.getNameBytes()).isNull();
		assertThat(joinHCol.getValueBytes()).isNull();
		assertThat(joinHCol.getNameSerializer()).isNull();
		assertThat(joinHCol.getValueSerializer()).isNull();

		joinHCol.apply("val2", 1, 20);

		assertThat(joinHCol.getName()).isEqualTo(11);
		assertThat(joinHCol.getValue()).isEqualTo("val2");
		assertThat(joinHCol.getTtl()).isEqualTo(20);
		assertThat(joinHCol.getClock()).isEqualTo(0);

		joinHCol.clear();

		assertThat(joinHCol.getName()).isNull();
		assertThat(joinHCol.getValue()).isNull();
		assertThat(joinHCol.getTtl()).isEqualTo(0);
		assertThat(joinHCol.getClock()).isEqualTo(0);

	}
}
