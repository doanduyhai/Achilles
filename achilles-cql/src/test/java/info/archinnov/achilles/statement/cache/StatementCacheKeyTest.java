package info.archinnov.achilles.statement.cache;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * PreparedStatementCacheKeyTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class StatementCacheKeyTest
{

	@Test
	public void should_be_equals() throws Exception
	{
		StatementCacheKey key1 = new StatementCacheKey(CacheType.SELECT_FIELD, "table",
				Sets.newHashSet("field1", "field2"), CompleteBean.class);
		StatementCacheKey key2 = new StatementCacheKey(CacheType.SELECT_FIELD, "table",
				Sets.newHashSet("field2", "field1"), CompleteBean.class);

		assertThat(key1).isEqualTo(key2);
	}
}
