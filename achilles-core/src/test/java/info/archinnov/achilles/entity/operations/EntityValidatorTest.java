package info.archinnov.achilles.entity.operations;

import static org.mockito.Mockito.when;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCounter;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.lang.reflect.Method;
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
import org.powermock.reflect.Whitebox;

/**
 * AchillesEntityValidatorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityValidatorTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private EntityValidator<PersistenceContext> entityValidator;

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private EntityProxifier<PersistenceContext> proxifier;

    @Mock
    private Map<Class<?>, EntityMeta> entityMetaMap;

    @Mock
    private EntityMeta entityMeta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta<Void, Long> idMeta;

    @Mock
    private PersistenceContext context;

    @Before
    public void setUp() {
        Whitebox.setInternalState(entityValidator, ReflectionInvoker.class, invoker);
        when((PropertyMeta<Void, Long>) entityMeta.getIdMeta()).thenReturn(idMeta);
    }

    @Test
    public void should_validate() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

        when((Class<CompleteBean>) proxifier.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
        when(invoker.getPrimaryKey(bean, idMeta)).thenReturn(12L);

        entityValidator.validateEntity(bean, entityMetaMap);
    }

    @Test
    public void should_exception_when_no_id() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

        when((Class<CompleteBean>) proxifier.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
        when(invoker.getPrimaryKey(bean, idMeta)).thenReturn(null);

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Cannot get primary key for entity " + CompleteBean.class.getCanonicalName());

        entityValidator.validateEntity(bean, entityMetaMap);
    }

    @Test
    public void should_validate_clustered_id() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
        CompoundKey clusteredId = new CompoundKey(11L, "name");

        when(invoker.getPrimaryKey(bean, idMeta)).thenReturn(clusteredId);
        when(idMeta.isEmbeddedId()).thenReturn(true);

        Method userIdGetter = CompoundKey.class.getMethod("getUserId");
        Method nameGetter = CompoundKey.class.getMethod("getName");

        when(idMeta.getComponentGetters()).thenReturn(Arrays.asList(userIdGetter, nameGetter));

        when(invoker.getValueFromField(clusteredId, userIdGetter)).thenReturn(11L);
        when(invoker.getValueFromField(clusteredId, nameGetter)).thenReturn("name");

        entityValidator.validateEntity(bean, entityMeta);
    }

    @Test
    public void should_validate_simple_id() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
        when(invoker.getPrimaryKey(bean, idMeta)).thenReturn(12L);
        when(idMeta.isEmbeddedId()).thenReturn(false);

        entityValidator.validateEntity(bean, entityMeta);
    }

    @Test
    public void should_validate_not_clustered_counter() throws Exception
    {
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
