package info.archinnov.achilles.dao;

import static org.fest.assertions.api.Assertions.assertThat;

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

		assertThat(pair1.equals(pair2)).isTrue();
		assertThat(pair1.toString()).isEqualTo("(12,test)");

		assertThat(pair1.hashCode()).isEqualTo(pair2.hashCode());
	}
}
