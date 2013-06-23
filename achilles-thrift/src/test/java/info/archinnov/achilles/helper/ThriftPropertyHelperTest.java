package info.archinnov.achilles.helper;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.CompoundKeyProperties;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.type.WideMap.BoundingMode;
import info.archinnov.achilles.type.WideMap.OrderingMode;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import mapping.entity.TweetCompoundKey;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import parser.entity.CompoundKey;
import parser.entity.CompoundKeyWithEnum;
import parser.entity.CorrectCompoundKey;
import com.google.common.collect.Maps;

/**
 * ThriftPropertyHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftPropertyHelperTest
{
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @InjectMocks
    private ThriftPropertyHelper helper;

    @Mock
    private MethodInvoker invoker;

    @Mock
    private PropertyMeta<TweetCompoundKey, String> compoundKeyWideMeta;

    @Mock
    private PropertyMeta<CompoundKeyWithEnum, String> compoundKeyWithEnumWideMeta;

    @Mock
    private CompoundKeyProperties compoundKeyProperties;

    @Mock
    private PropertyMeta<CorrectCompoundKey, String> multiKeyWideMapMeta;

    @Mock
    private PropertyMeta<Integer, String> wideMapMeta;

    @Mock
    private List<Method> componentGetters;

    @Before
    public void setUp()
    {
        when(wideMapMeta.isSingleKey()).thenReturn(true);
        when(multiKeyWideMapMeta.isSingleKey()).thenReturn(false);
    }

    @Test
    public void should_determine_composite_type_alias_for_column_family_check() throws Exception
    {
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
        propertyMeta.setType(PropertyType.WIDE_MAP);
        propertyMeta.setKeyClass(Integer.class);
        Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
        propertyMap.put("map", propertyMeta);
        entityMeta.setPropertyMetas(propertyMap);

        String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
                propertyMeta, false);

        assertThat(compatatorTypeAlias).isEqualTo(
                "CompositeType(org.apache.cassandra.db.marshal.BytesType)");
    }

    @Test
    public void should_determine_composite_type_alias_for_column_family() throws Exception
    {
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
        propertyMeta.setType(PropertyType.WIDE_MAP);
        propertyMeta.setKeyClass(Integer.class);
        Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
        propertyMap.put("map", propertyMeta);
        entityMeta.setPropertyMetas(propertyMap);

        String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
                propertyMeta, true);

        assertThat(compatatorTypeAlias).isEqualTo("(BytesType)");
    }

    @Test
    public void should_determine_composite_type_alias_for_multikey_column_family() throws Exception
    {
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta<TweetCompoundKey, String> propertyMeta = new PropertyMeta<TweetCompoundKey, String>();
        propertyMeta.setType(PropertyType.WIDE_MAP);
        propertyMeta.setKeyClass(TweetCompoundKey.class);
        Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
        propertyMap.put("values", propertyMeta);
        entityMeta.setPropertyMetas(propertyMap);

        String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
                propertyMeta, true);

        assertThat(compatatorTypeAlias).isEqualTo("(UUIDType,UTF8Type,BytesType)");
    }

    @Test
    public void should_determine_composite_type_alias_for_multikey_column_family_check()
            throws Exception
    {
        EntityMeta entityMeta = new EntityMeta();
        PropertyMeta<TweetCompoundKey, String> propertyMeta = new PropertyMeta<TweetCompoundKey, String>();
        propertyMeta.setType(PropertyType.WIDE_MAP);
        propertyMeta.setKeyClass(TweetCompoundKey.class);
        Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
        propertyMap.put("values", propertyMeta);
        entityMeta.setPropertyMetas(propertyMap);

        String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
                propertyMeta, false);

        assertThat(compatatorTypeAlias)
                .isEqualTo(
                        "CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.BytesType)");
    }

    // Ascending order
    @Test
    public void should_return_determine_equalities_for_inclusive_start_and_end_asc()
            throws Exception
    {
        ComponentEquality[] equality = helper.determineEquality(BoundingMode.INCLUSIVE_BOUNDS,
                OrderingMode.ASCENDING);
        assertThat(equality[0]).isEqualTo(EQUAL);
        assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
    }

    @Test
    public void should_return_determine_equalities_for_exclusive_start_and_end_asc()
            throws Exception
    {
        ComponentEquality[] equality = helper.determineEquality(BoundingMode.EXCLUSIVE_BOUNDS,
                OrderingMode.ASCENDING);
        assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
        assertThat(equality[1]).isEqualTo(LESS_THAN_EQUAL);
    }

    @Test
    public void should_return_determine_equalities_for_inclusive_start_exclusive_end_asc()
            throws Exception
    {
        ComponentEquality[] equality = helper.determineEquality(
                BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.ASCENDING);
        assertThat(equality[0]).isEqualTo(EQUAL);
        assertThat(equality[1]).isEqualTo(LESS_THAN_EQUAL);
    }

    @Test
    public void should_return_determine_equalities_for_exclusive_start_inclusive_end_asc()
            throws Exception
    {
        ComponentEquality[] equality = helper.determineEquality(
                BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.ASCENDING);
        assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
        assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
    }

    // Descending order
    @Test
    public void should_return_determine_equalities_for_inclusive_start_and_end_desc()
            throws Exception
    {
        ComponentEquality[] equality = helper.determineEquality(BoundingMode.INCLUSIVE_BOUNDS,
                OrderingMode.DESCENDING);
        assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
        assertThat(equality[1]).isEqualTo(EQUAL);
    }

    @Test
    public void should_return_determine_equalities_for_exclusive_start_and_end_desc()
            throws Exception
    {
        ComponentEquality[] equality = helper.determineEquality(BoundingMode.EXCLUSIVE_BOUNDS,
                OrderingMode.DESCENDING);
        assertThat(equality[0]).isEqualTo(LESS_THAN_EQUAL);
        assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
    }

    @Test
    public void should_return_determine_equalities_for_inclusive_start_exclusive_end_desc()
            throws Exception
    {
        ComponentEquality[] equality = helper.determineEquality(
                BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.DESCENDING);
        assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
        assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
    }

    @Test
    public void should_return_determine_equalities_for_exclusive_start_inclusive_end_desc()
            throws Exception
    {
        ComponentEquality[] equality = helper.determineEquality(
                BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);
        assertThat(equality[0]).isEqualTo(LESS_THAN_EQUAL);
        assertThat(equality[1]).isEqualTo(EQUAL);
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
    public void should_validate_compound_key_bounds() throws Exception
    {
        CorrectCompoundKey start = new CorrectCompoundKey();
        CorrectCompoundKey end = new CorrectCompoundKey();

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
        CorrectCompoundKey start = new CorrectCompoundKey();
        CorrectCompoundKey end = new CorrectCompoundKey();

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
        CorrectCompoundKey start = new CorrectCompoundKey();
        CorrectCompoundKey end = new CorrectCompoundKey();

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
        CorrectCompoundKey start = new CorrectCompoundKey();
        CorrectCompoundKey end = new CorrectCompoundKey();

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
        CorrectCompoundKey start = new CorrectCompoundKey();
        CorrectCompoundKey end = new CorrectCompoundKey();

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
