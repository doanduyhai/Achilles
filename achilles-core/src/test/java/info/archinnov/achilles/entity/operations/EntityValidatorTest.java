package info.archinnov.achilles.entity.operations;

import static org.mockito.Mockito.when;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
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
    private EntityValidator<PersistenceContext> achillesEntityValidator;

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
        Whitebox.setInternalState(achillesEntityValidator, "invoker", invoker);
        when((PropertyMeta<Void, Long>) entityMeta.getIdMeta()).thenReturn(idMeta);
    }

    @Test
    public void should_validate() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

        when((Class<CompleteBean>) proxifier.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
        when(invoker.getPrimaryKey(bean, idMeta)).thenReturn(12L);

        achillesEntityValidator.validateEntity(bean, entityMetaMap);
    }

    @Test
    public void should_exception_when_no_id() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

        when((Class<CompleteBean>) proxifier.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
        when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
        when(invoker.getPrimaryKey(bean, idMeta)).thenReturn(null);

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Cannot get primary key for entity " + CompleteBean.class.getCanonicalName());

        achillesEntityValidator.validateEntity(bean, entityMetaMap);
    }

    @Test
    public void should_validate_clustered_id() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
        CompoundKey clusteredId = new CompoundKey(11L, "name");

        when(invoker.getPrimaryKey(bean, idMeta)).thenReturn(clusteredId);
        when(idMeta.isSingleKey()).thenReturn(false);

        Method userIdGetter = CompoundKey.class.getMethod("getUserId");
        Method nameGetter = CompoundKey.class.getMethod("getName");

        when(idMeta.getComponentGetters()).thenReturn(Arrays.asList(userIdGetter, nameGetter));

        when(invoker.getValueFromField(clusteredId, userIdGetter)).thenReturn(11L);
        when(invoker.getValueFromField(clusteredId, nameGetter)).thenReturn("name");

        achillesEntityValidator.validateEntity(bean, entityMeta);
    }

    @Test
    public void should_validate_simple_id() throws Exception {
        CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
        when(invoker.getPrimaryKey(bean, idMeta)).thenReturn(12L);
        when(idMeta.isSingleKey()).thenReturn(true);

        achillesEntityValidator.validateEntity(bean, entityMeta);

    }

}
