package info.archinnov.achilles.entity.manager;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * CQLEntityManagerFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityManagerFactoryTest
{

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private CQLEntityManagerFactory factory;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private AchillesConsistencyLevelPolicy policy;

    @Test
    public void should_create_entity_manager() throws Exception
    {
        when(configContext.getConsistencyPolicy()).thenReturn(policy);
        factory.setConfigContext(configContext);
        CQLEntityManager em = factory.createEntityManager();
        assertThat(em).isNotNull();
    }
}
