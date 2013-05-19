package info.archinnov.achilles.entity.context;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * DaoContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class DaoContextTest
{
	private DaoContext context;

	private Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();
	private Map<String, ThriftGenericWideRowDao> columnFamilyDaosMap = new HashMap<String, ThriftGenericWideRowDao>();

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private ThriftGenericWideRowDao columnFamilyDao;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Before
	public void setUp()
	{
		context = new DaoContext(entityDaosMap, columnFamilyDaosMap, thriftCounterDao);
	}

	@Test
	public void should_get_counter_dao() throws Exception
	{
		assertThat(context.getCounterDao()).isSameAs(thriftCounterDao);
	}

	@Test
	public void should_get_entity_dao() throws Exception
	{
		entityDaosMap.put("dao", entityDao);
		assertThat((ThriftGenericEntityDao) context.findEntityDao("dao")).isSameAs(entityDao);
	}

	@Test
	public void should_get_wide_row_dao() throws Exception
	{
		columnFamilyDaosMap.put("dao", columnFamilyDao);
		assertThat((ThriftGenericWideRowDao) context.findWideRowDao("dao")).isSameAs(
				columnFamilyDao);
	}
}
