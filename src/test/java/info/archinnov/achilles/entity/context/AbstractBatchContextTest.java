package info.archinnov.achilles.entity.context;

import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;

import java.util.Map;

import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AbstractBatchContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class AbstractBatchContextTest
{

	@InjectMocks
	private FlushContext context;

	@Mock
	private Map<String, GenericEntityDao<?>> entityDaosMap;

	@Mock
	private Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap;

	@Mock
	private CounterDao counterDao;
}
