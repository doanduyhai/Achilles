package info.archinnov.achilles.helper;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.CompoundKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;
import info.archinnov.achilles.type.WideMap.OrderingMode;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import parser.entity.CompoundKey;
import parser.entity.CorrectMultiKey;
import parser.entity.MultiKeyIncorrectType;
import parser.entity.MultiKeyNotInstantiable;
import parser.entity.MultiKeyWithDuplicateOrder;
import parser.entity.MultiKeyWithNegativeOrder;
import parser.entity.MultiKeyWithNoAnnotation;

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

    @Mock
    private PropertyMeta<Integer, String> wideMapMeta;

    @Mock
    private PropertyMeta<CorrectMultiKey, String> multiKeyWideMapMeta;

    @Before
    public void setUp()
    {
        when(wideMapMeta.isSingleKey()).thenReturn(true);
        when(multiKeyWideMapMeta.isSingleKey()).thenReturn(false);
    }

    @Test
    public void should_parse_multi_key() throws Exception
    {
        Method nameGetter = CorrectMultiKey.class.getMethod("getName");
        Method nameSetter = CorrectMultiKey.class.getMethod("setName", String.class);

        Method rankGetter = CorrectMultiKey.class.getMethod("getRank");
        Method rankSetter = CorrectMultiKey.class.getMethod("setRank", int.class);

        CompoundKeyProperties props = helper.parseCompoundKey(CorrectMultiKey.class);

        assertThat(props.getComponentGetters()).containsExactly(nameGetter, rankGetter);
        assertThat(props.getComponentSetters()).containsExactly(nameSetter, rankSetter);
        assertThat(props.getComponentClasses()).containsExactly(String.class, int.class);

    }

    @Test
    public void should_exception_when_multi_key_incorrect_type() throws Exception
    {
        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx
                .expectMessage("The class 'java.util.List' is not a valid key type for the MultiKey class '"
                        + MultiKeyIncorrectType.class.getCanonicalName() + "'");

        helper.parseCompoundKey(MultiKeyIncorrectType.class);
    }

    @Test
    public void should_exception_when_multi_key_wrong_key_order() throws Exception
    {
        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("The key orders is wrong for MultiKey class '"
                + MultiKeyWithNegativeOrder.class.getCanonicalName() + "'");

        helper.parseCompoundKey(MultiKeyWithNegativeOrder.class);
    }

    @Test
    public void should_exception_when_multi_key_has_no_annotation() throws Exception
    {
        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("No field with @Order annotation found in the class '"
                + MultiKeyWithNoAnnotation.class.getCanonicalName() + "'");

        helper.parseCompoundKey(MultiKeyWithNoAnnotation.class);
    }

    @Test
    public void should_exception_when_multi_key_has_duplicate_order() throws Exception
    {
        expectedEx.expect(AchillesBeanMappingException.class);

        expectedEx.expectMessage("The order '1' is duplicated in MultiKey class '"
                + MultiKeyWithDuplicateOrder.class.getCanonicalName() + "'");

        helper.parseCompoundKey(MultiKeyWithDuplicateOrder.class);
    }

    @Test
    public void should_exception_when_multi_key_not_instantiable() throws Exception
    {
        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("The class '" + MultiKeyNotInstantiable.class.getCanonicalName()
                + "' should have a public default constructor");

        helper.parseCompoundKey(MultiKeyNotInstantiable.class);
    }

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

    @Test
    public void should_validate_no_hole() throws Exception
    {
        List<Object> keyValues = Arrays.asList((Object) "a", "b", null, null);

        int lastNotNullIndex = helper.findLastNonNullIndexForComponents("sdfsdf", keyValues);

        assertThat(lastNotNullIndex).isEqualTo(1);

    }

    @Test(expected = IllegalArgumentException.class)
    public void should_exception_when_hole() throws Exception
    {
        List<Object> keyValues = Arrays.asList((Object) "a", null, "b");

        helper.findLastNonNullIndexForComponents("sdfsdf", keyValues);
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_exception_when_starting_with_hole() throws Exception
    {
        List<Object> keyValues = Arrays.asList((Object) null, "a", "b");

        helper.findLastNonNullIndexForComponents("sdfsdf", keyValues);
    }

    @Test
    public void should_validate_bounds() throws Exception
    {
        helper.checkBounds(wideMapMeta, 12, 15, OrderingMode.ASCENDING, false);
    }

    @Test
    public void should_validate_asc_bounds_with_start_null() throws Exception
    {
        helper.checkBounds(wideMapMeta, null, 15, OrderingMode.ASCENDING, false);
    }

    @Test
    public void should_validate_asc_bounds_with_end_null() throws Exception
    {
        helper.checkBounds(wideMapMeta, 12, null, OrderingMode.ASCENDING, false);
    }

    @Test
    public void should_exception_when_asc_start_greater_than_end() throws Exception
    {

        expectedEx.expect(AchillesException.class);
        expectedEx.expectMessage("For range query, start value should be lesser or equal to end");

        helper.checkBounds(wideMapMeta, 15, 12, OrderingMode.ASCENDING, false);
    }

    @Test
    public void should_exception_when_desc_start_lesser_than_end() throws Exception
    {

        expectedEx.expect(AchillesException.class);
        expectedEx
                .expectMessage("For reverse range query, start value should be greater or equal to end value");

        helper.checkBounds(wideMapMeta, 12, 15, OrderingMode.DESCENDING, false);
    }

    @Test
    public void should_validate_multi_key_bounds() throws Exception
    {
        CorrectMultiKey start = new CorrectMultiKey();
        CorrectMultiKey end = new CorrectMultiKey();

        List<Object> startComponentValues = Arrays.asList((Object) "abc", 12);
        List<Object> endComponentValues = Arrays.asList((Object) "abc", 20);

        when(multiKeyWideMapMeta.getComponentGetters()).thenReturn(componentGetters);
        when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
        when(invoker.extractCompoundKeyComponents(start, componentGetters)).thenReturn(
                startComponentValues);
        when(invoker.extractCompoundKeyComponents(end, componentGetters)).thenReturn(endComponentValues);

        helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.ASCENDING, false);
    }

    @Test
    public void should_validate_multi_key_asc_bounds_with_nulls() throws Exception
    {
        CorrectMultiKey start = new CorrectMultiKey();
        CorrectMultiKey end = new CorrectMultiKey();

        List<Object> startComponentValues = Arrays.asList((Object) "abc", null);
        List<Object> endComponentValues = Arrays.asList((Object) "abd", null);

        when(multiKeyWideMapMeta.getComponentGetters()).thenReturn(componentGetters);
        when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
        when(invoker.extractCompoundKeyComponents(start, componentGetters)).thenReturn(
                startComponentValues);
        when(invoker.extractCompoundKeyComponents(end, componentGetters)).thenReturn(endComponentValues);

        helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.ASCENDING, false);
    }

    @Test
    public void should_exception_when_multi_key_asc_start_greater_than_end() throws Exception
    {
        CorrectMultiKey start = new CorrectMultiKey();
        CorrectMultiKey end = new CorrectMultiKey();

        List<Object> startComponentValues = Arrays.asList((Object) "abc", 12);
        List<Object> endComponentValues = Arrays.asList((Object) "abc", 10);

        when(multiKeyWideMapMeta.getComponentGetters()).thenReturn(componentGetters);
        when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
        when(invoker.extractCompoundKeyComponents(start, componentGetters)).thenReturn(
                startComponentValues);
        when(invoker.extractCompoundKeyComponents(end, componentGetters)).thenReturn(endComponentValues);

        expectedEx.expect(AchillesException.class);
        expectedEx
                .expectMessage("For multiKey ascending range query, startKey value should be lesser or equal to end endKey");

        helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.ASCENDING, false);
    }

    @Test
    public void should_exception_when_multi_key_asc_hole_in_start() throws Exception
    {
        CorrectMultiKey start = new CorrectMultiKey();
        CorrectMultiKey end = new CorrectMultiKey();

        List<Object> startComponentValues = Arrays.asList((Object) null, 10);
        List<Object> endComponentValues = Arrays.asList((Object) "abc", 10);

        when(multiKeyWideMapMeta.getComponentGetters()).thenReturn(componentGetters);
        when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
        when(invoker.extractCompoundKeyComponents(start, componentGetters)).thenReturn(
                startComponentValues);
        when(invoker.extractCompoundKeyComponents(end, componentGetters)).thenReturn(endComponentValues);
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx
                .expectMessage("There should not be any null value between two non-null keys of WideMap 'any_property'");

        helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.ASCENDING, false);
    }

    @Test
    public void should_exception_when_multi_key_desc_start_lesser_than_end() throws Exception
    {
        CorrectMultiKey start = new CorrectMultiKey();
        CorrectMultiKey end = new CorrectMultiKey();

        List<Object> startComponentValues = Arrays.asList((Object) "abc", 12);
        List<Object> endComponentValues = Arrays.asList((Object) "def", 10);

        when(multiKeyWideMapMeta.getComponentGetters()).thenReturn(componentGetters);
        when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
        when(invoker.extractCompoundKeyComponents(start, componentGetters)).thenReturn(
                startComponentValues);
        when(invoker.extractCompoundKeyComponents(end, componentGetters)).thenReturn(endComponentValues);

        expectedEx.expect(AchillesException.class);
        expectedEx
                .expectMessage("For multiKey descending range query, startKey value should be greater or equal to end endKey");

        helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.DESCENDING, false);
    }

    @Test
    public void should_exception_when_null_partition_key_for_start_clustering_key()
            throws Exception
    {
        CompoundKey start = new CompoundKey(null, "name1");
        CompoundKey end = new CompoundKey(10L, "name2");

        List<Object> startComponentValues = Arrays.asList((Object) null, "name1");
        List<Object> endComponentValues = Arrays.asList((Object) 10L, "name2");

        when(multiKeyWideMapMeta.getComponentGetters()).thenReturn(componentGetters);
        when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
        when(invoker.extractCompoundKeyComponents(start, componentGetters)).thenReturn(
                startComponentValues);
        when(invoker.extractCompoundKeyComponents(end, componentGetters)).thenReturn(endComponentValues);

        expectedEx.expect(AchillesException.class);
        expectedEx
                .expectMessage("Partition key should not be null for start clustering key : [null, name1]");

        helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.ASCENDING, true);
    }

    @Test
    public void should_exception_when_null_partition_key_for_end_clustering_key() throws Exception
    {
        CompoundKey start = new CompoundKey(10L, "name1");
        CompoundKey end = new CompoundKey(null, "name2");

        List<Object> startComponentValues = Arrays.asList((Object) 10L, "name1");
        List<Object> endComponentValues = Arrays.asList((Object) null, "name2");

        when(multiKeyWideMapMeta.getComponentGetters()).thenReturn(componentGetters);
        when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
        when(invoker.extractCompoundKeyComponents(start, componentGetters)).thenReturn(
                startComponentValues);
        when(invoker.extractCompoundKeyComponents(end, componentGetters)).thenReturn(endComponentValues);

        expectedEx.expect(AchillesException.class);
        expectedEx
                .expectMessage("Partition key should not be null for end clustering key : [null, name2]");

        helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.ASCENDING, true);
    }

    @Test
    public void should_exception_when_partition_keys_not_equal() throws Exception
    {
        CompoundKey start = new CompoundKey(10L, "name1");
        CompoundKey end = new CompoundKey(11L, "name2");

        List<Object> startComponentValues = Arrays.asList((Object) 10L, "name1");
        List<Object> endComponentValues = Arrays.asList((Object) 11L, "name2");

        when(multiKeyWideMapMeta.getComponentGetters()).thenReturn(componentGetters);
        when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
        when(invoker.extractCompoundKeyComponents(start, componentGetters)).thenReturn(
                startComponentValues);
        when(invoker.extractCompoundKeyComponents(end, componentGetters)).thenReturn(endComponentValues);

        expectedEx.expect(AchillesException.class);
        expectedEx
                .expectMessage("Partition key should be equals for start and end clustering keys : [[10, name1],[11, name2]]");

        helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.ASCENDING, true);
    }
}
