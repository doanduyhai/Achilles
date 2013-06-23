package info.archinnov.achilles.helper;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesPropertyHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class PropertyHelperTest
{
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @InjectMocks
    private PropertyHelper helper;

    @Mock
    private MethodInvoker invoker;

    @Mock
    private AchillesConsistencyLevelPolicy policy;

    @Mock
    private List<Method> componentGetters;

    @Test
    public void should_infer_value_class_from_list() throws Exception
    {
        @SuppressWarnings("unused")
        class Test
        {
            private List<String> friends;
        }

        Type type = Test.class.getDeclaredField("friends").getGenericType();

        Class<String> infered = helper.inferValueClassForListOrSet(type, Test.class);

        assertThat(infered).isEqualTo(String.class);
    }

    @Test
    public void should_exception_when_infering_value_type_from_raw_list() throws Exception
    {
        @SuppressWarnings(
        {
                "rawtypes",
                "unused"
        })
        class Test
        {
            private List friends;
        }

        Type type = Test.class.getDeclaredField("friends").getGenericType();

        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("The type '" + type.getClass().getCanonicalName()
                + "' of the entity 'null' should be parameterized");

        helper.inferValueClassForListOrSet(type, Test.class);

    }

    @Test
    public void should_find_lazy() throws Exception
    {

        class Test
        {
            @Lazy
            private String name;
        }

        Field field = Test.class.getDeclaredField("name");

        assertThat(helper.isLazy(field)).isTrue();
    }

    @Test
    public void should_check_consistency_annotation() throws Exception
    {
        class Test
        {
            @Consistency
            private String consistency;
        }

        Field field = Test.class.getDeclaredField("consistency");

        assertThat(helper.hasConsistencyAnnotation(field)).isTrue();
    }

    @Test
    public void should_not_find_counter_if_not_long_type() throws Exception
    {

    }

    @Test
    public void should_find_any_any_consistency_level() throws Exception
    {
        class Test
        {
            @Consistency(read = ANY, write = LOCAL_QUORUM)
            private WideMap<Integer, String> field;
        }

        when(policy.getDefaultGlobalReadConsistencyLevel()).thenReturn(ONE);
        when(policy.getDefaultGlobalWriteConsistencyLevel()).thenReturn(ONE);

        Pair<ConsistencyLevel, ConsistencyLevel> levels = helper.findConsistencyLevels(
                Test.class.getDeclaredField("field"), policy);

        assertThat(levels.left).isEqualTo(ANY);
        assertThat(levels.right).isEqualTo(LOCAL_QUORUM);
    }

    @Test
    public void should_find_quorum_consistency_level_by_default() throws Exception
    {
        class Test
        {
            @SuppressWarnings("unused")
            private WideMap<Integer, String> field;
        }

        when(policy.getDefaultGlobalReadConsistencyLevel()).thenReturn(ConsistencyLevel.QUORUM);
        when(policy.getDefaultGlobalWriteConsistencyLevel()).thenReturn(ConsistencyLevel.QUORUM);

        Pair<ConsistencyLevel, ConsistencyLevel> levels = helper.findConsistencyLevels(
                Test.class.getDeclaredField("field"), policy);

        assertThat(levels.left).isEqualTo(ConsistencyLevel.QUORUM);
        assertThat(levels.right).isEqualTo(ConsistencyLevel.QUORUM);
    }

    @Test
    public void should_return_true_when_type_supported() throws Exception
    {
        assertThat(PropertyHelper.isSupportedType(Long.class)).isTrue();
    }

}
