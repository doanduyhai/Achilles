package info.archinnov.achilles.entity.operations;

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntityWithCounter;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

@RunWith(MockitoJUnitRunner.class)
public class EntityValidatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private EntityValidator entityValidator;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private Map<Class<?>, EntityMeta> entityMetaMap;

    @Mock
    private EntityMeta entityMeta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    @Mock
    private PersistenceContext context;

    @Before
    public void setUp() {
        when(entityMeta.getIdMeta()).thenReturn(idMeta);
    }

    @Test
    public void should_validate() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

        when((Class<CompleteBean>) proxifier.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
        when(entityMeta.getPrimaryKey(bean)).thenReturn(12L);

        entityValidator.validateEntity(bean, entityMetaMap);
    }

    @Test
    public void should_exception_when_no_id() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

        when((Class<CompleteBean>) proxifier.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
        when(entityMeta.getPrimaryKey(bean)).thenReturn(null);

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Cannot get primary key for entity " + CompleteBean.class.getCanonicalName());

        entityValidator.validateEntity(bean, entityMetaMap);
    }

    @Test
    public void should_validate_clustered_id() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
        EmbeddedKey clusteredId = new EmbeddedKey(11L, "name");

        when(entityMeta.getPrimaryKey(bean)).thenReturn(clusteredId);
        when(idMeta.isEmbeddedId()).thenReturn(true);
        when(idMeta.encodeToComponents(clusteredId)).thenReturn(Arrays.<Object> asList(11L, "name"));
        entityValidator.validateEntity(bean, entityMeta);
    }

    @Test
    public void should_validate_simple_id() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
        when(entityMeta.getPrimaryKey(bean)).thenReturn(12L);
        when(idMeta.isEmbeddedId()).thenReturn(false);

        entityValidator.validateEntity(bean, entityMeta);
    }

    @Test
    public void should_validate_not_clustered_counter() throws Exception {
        ClusteredEntityWithCounter entity = new ClusteredEntityWithCounter();
        when((Class<ClusteredEntityWithCounter>) proxifier.deriveBaseClass(entity)).thenReturn(
                ClusteredEntityWithCounter.class);
        when(entityMetaMap.get(ClusteredEntityWithCounter.class)).thenReturn(entityMeta);
        when(entityMeta.isClusteredCounter()).thenReturn(false);
        entityValidator.validateNotClusteredCounter(entity, entityMetaMap);
    }

    @Test
    public void should_exception_when_not_clustered_counter() throws Exception {
        ClusteredEntityWithCounter entity = new ClusteredEntityWithCounter();
        when((Class<ClusteredEntityWithCounter>) proxifier.deriveBaseClass(entity)).thenReturn(
                ClusteredEntityWithCounter.class);
        when(entityMetaMap.get(ClusteredEntityWithCounter.class)).thenReturn(entityMeta);
        when(entityMeta.isClusteredCounter()).thenReturn(true);

        exception.expect(AchillesException.class);
        exception.expectMessage("The entity '" + entity
                                        + "' is a clustered counter and does not support insert/update with TTL");

        entityValidator.validateNotClusteredCounter(entity, entityMetaMap);
    }

}
