package info.archinnov.achilles.type;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.type.Pair;

import org.junit.Test;

/**
 * PairTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class PairTest
{

	@Test
	public void should_test_all() throws Exception
	{
		Pair<Integer, String> pair1 = Pair.create(12, "test");
		Pair<Integer, String> pair2 = Pair.create(12, "test");
		Pair<Integer, String> pair3 = Pair.create(13, "test");
		Pair<Integer, String> pair4 = Pair.create(12, "tesu");

		assertThat(pair1.equals(pair2)).isTrue();
		assertThat(pair1.toString()).isEqualTo("(12,test)");

		assertThat(pair1.hashCode()).isEqualTo(pair2.hashCode());

		assertThat(Pair.create(null, null).hashCode()).isEqualTo(31 * 31);

		assertThat(pair1.equals(pair3)).isFalse();
		assertThat(pair1.equals(pair4)).isFalse();
	}
}
