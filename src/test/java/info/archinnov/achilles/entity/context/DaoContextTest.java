package info.archinnov.achilles.entity.context;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;

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

	private Map<String, GenericEntityDao<?>> entityDaosMap = new HashMap<String, GenericEntityDao<?>>();
	private Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap = new HashMap<String, GenericColumnFamilyDao<?, ?>>();

	@Mock
	private GenericEntityDao<?> entityDao;

	@Mock
	private GenericColumnFamilyDao<?, ?> columnFamilyDao;

	@Mock
	private CounterDao counterDao;

	@Before
	public void setUp()
	{
		context = new DaoContext(entityDaosMap, columnFamilyDaosMap, counterDao);
	}

	@Test
	public void should_get_counter_dao() throws Exception
	{
		assertThat(context.getCounterDao()).isSameAs(counterDao);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_get_entity_dao() throws Exception
	{
		entityDaosMap.put("dao", entityDao);
		assertThat((GenericEntityDao) context.findEntityDao("dao")).isSameAs(entityDao);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_get_column_family_dao() throws Exception
	{
		columnFamilyDaosMap.put("dao", columnFamilyDao);
		assertThat((GenericColumnFamilyDao) context.findColumnFamilyDao("dao")).isSameAs(
				columnFamilyDao);
	}
}
