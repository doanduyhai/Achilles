package info.archinnov.achilles.composite.factory;

import static info.archinnov.achilles.serializer.SerializerUtils.INT_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.UUID_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import info.archinnov.achilles.entity.type.WideMap.OrderingMode;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.helper.CompositeHelper;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import mapping.entity.TweetMultiKey;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * CompositeKeyFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class CompositeKeyFactoryTest {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @InjectMocks
    private CompositeKeyFactory factory;

    @Mock
    private CompositeHelper helper;

    @Mock
    private EntityIntrospector entityIntrospector;

    @Mock
    private PropertyMeta<Integer, String> wideMapMeta;

    @Mock
    private PropertyMeta<TweetMultiKey, String> multiKeyWideMapMeta;

    @Mock
    private MultiKeyProperties multiKeyProperties;

    @Before
    public void setUp() {
        when(wideMapMeta.isSingleKey()).thenReturn(true);
        when(wideMapMeta.getPropertyName()).thenReturn("property");
        when(wideMapMeta.getKeySerializer()).thenReturn(INT_SRZ);

        when(multiKeyWideMapMeta.isSingleKey()).thenReturn(false);
        when(multiKeyWideMapMeta.getPropertyName()).thenReturn("property");
        when(multiKeyWideMapMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
    }

    @Test
    public void should_create_for_insert() throws Exception {
        Composite comp = factory.createBaseComposite(wideMapMeta, 12);

        assertThat(comp.getComponents()).hasSize(1);
        assertThat((Integer) comp.getComponents().get(0).getValue()).isEqualTo(12);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_create_multikey_for_insert() throws Exception {
        List<Method> componentGetters = mock(List.class);
        TweetMultiKey tweetMultiKey = new TweetMultiKey();
        List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ, UUID_SRZ);
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        List<Object> keyValues = Arrays.asList((Object) 1, "a", uuid);

        when(multiKeyProperties.getComponentSerializers()).thenReturn(serializers);
        when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
        when(entityIntrospector.determineMultiKey(tweetMultiKey, componentGetters)).thenReturn(keyValues);

        Composite comp = factory.createBaseComposite(multiKeyWideMapMeta, tweetMultiKey);

        assertThat(comp.getComponents()).hasSize(3);
        assertThat((Integer) comp.getComponents().get(0).getValue()).isEqualTo(1);
        assertThat((String) comp.getComponents().get(1).getValue()).isEqualTo("a");
        assertThat((UUID) comp.getComponents().get(2).getValue()).isEqualTo(uuid);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_exception_when_missing_value() throws Exception {
        TweetMultiKey tweetMultiKey = new TweetMultiKey();
        List<Method> componentGetters = mock(List.class);
        List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ, UUID_SRZ);
        List<Object> keyValues = Arrays.asList((Object) 1, "a");

        when(multiKeyProperties.getComponentSerializers()).thenReturn(serializers);
        when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
        when(entityIntrospector.determineMultiKey(tweetMultiKey, componentGetters)).thenReturn(keyValues);

        expectedEx.expect(AchillesException.class);
        expectedEx.expectMessage("There should be 3 values for the key of WideMap 'property'");

        factory.createBaseComposite(multiKeyWideMapMeta, tweetMultiKey);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_exception_when_null_value() throws Exception {
        TweetMultiKey tweetMultiKey = new TweetMultiKey();
        List<Method> componentGetters = mock(List.class);
        List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ, UUID_SRZ);
        List<Object> keyValues = Arrays.asList((Object) 1, "a", null);

        when(multiKeyProperties.getComponentSerializers()).thenReturn(serializers);
        when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
        when(entityIntrospector.determineMultiKey(tweetMultiKey, componentGetters)).thenReturn(keyValues);

        expectedEx.expect(AchillesException.class);
        expectedEx.expectMessage("The values for the for the key of WideMap 'property' should not be null");

        factory.createBaseComposite(multiKeyWideMapMeta, tweetMultiKey);

    }

    @Test
    public void should_create_for_query() throws Exception {

        Composite comp = factory.createForQuery(wideMapMeta, 123, LESS_THAN_EQUAL);

        assertThat(comp.getComponents()).hasSize(1);
        assertThat((Integer) comp.getComponents().get(0).getValue()).isEqualTo(123);
        assertThat(comp.getComponents().get(0).getEquality()).isEqualTo(LESS_THAN_EQUAL);
    }

    @Test
    public void should_create_null_for_query() throws Exception {
        Composite comp = factory.createForQuery(wideMapMeta, null, LESS_THAN_EQUAL);
        assertThat(comp).isNull();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_create_multikey_for_query() throws Exception {
        TweetMultiKey tweetMultiKey = new TweetMultiKey();
        List<Method> componentGetters = mock(List.class);
        List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ, UUID_SRZ);
        List<Object> keyValues = Arrays.asList((Object) 1, "a", null);

        when(multiKeyProperties.getComponentSerializers()).thenReturn(serializers);
        when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
        when(entityIntrospector.determineMultiKey(tweetMultiKey, componentGetters)).thenReturn(keyValues);

        when(helper.findLastNonNullIndexForComponents("property", keyValues)).thenReturn(1);

        Composite comp = factory.createForQuery(multiKeyWideMapMeta, tweetMultiKey, LESS_THAN_EQUAL);

        assertThat(comp.getComponents()).hasSize(2);

        assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
        assertThat(comp.getComponent(0).getValue()).isEqualTo(1);

        assertThat(comp.getComponent(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);
        assertThat(comp.getComponent(1).getValue()).isEqualTo("a");

    }

    @Test
    public void should_create_composites_for_query() throws Exception {

        when(helper.determineEquality(BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.ASCENDING)) //
                .thenReturn(new ComponentEquality[] { EQUAL, LESS_THAN_EQUAL });
        Composite[] composites = factory.createForQuery(wideMapMeta, 12, 15, BoundingMode.INCLUSIVE_START_BOUND_ONLY,
                OrderingMode.ASCENDING);

        assertThat(composites).hasSize(2);
        assertThat(composites[0].getComponent(0).getEquality()).isEqualTo(EQUAL);
        assertThat(composites[0].getComponent(0).getValue()).isEqualTo(12);
        assertThat(composites[1].getComponent(0).getEquality()).isEqualTo(LESS_THAN_EQUAL);
        assertThat(composites[1].getComponent(0).getValue()).isEqualTo(15);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_create_multikey_composites_for_query() throws Exception {
        TweetMultiKey tweetKey1 = new TweetMultiKey();
        TweetMultiKey tweetKey2 = new TweetMultiKey();
        List<Method> componentGetters = mock(List.class);
        List<Serializer<?>> serializers = Arrays.asList((Serializer<?>) INT_SRZ, STRING_SRZ, UUID_SRZ);
        List<Object> keyValues1 = Arrays.asList((Object) 1, "a", null);
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        List<Object> keyValues2 = Arrays.asList((Object) 5, "c", uuid);

        when(helper.determineEquality(BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.ASCENDING)) //
                .thenReturn(new ComponentEquality[] { LESS_THAN_EQUAL, GREATER_THAN_EQUAL });
        when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
        when(entityIntrospector.determineMultiKey(tweetKey1, componentGetters)).thenReturn(keyValues1);
        when(entityIntrospector.determineMultiKey(tweetKey2, componentGetters)).thenReturn(keyValues2);

        when(multiKeyProperties.getComponentSerializers()).thenReturn(serializers);

        when(helper.findLastNonNullIndexForComponents("property", keyValues1)).thenReturn(1);
        when(helper.findLastNonNullIndexForComponents("property", keyValues2)).thenReturn(2);

        Composite[] composites = factory.createForQuery(
                //
                multiKeyWideMapMeta, tweetKey1, tweetKey2, BoundingMode.INCLUSIVE_END_BOUND_ONLY,
                OrderingMode.ASCENDING);

        assertThat(composites).hasSize(2);
        assertThat(composites[0].getComponent(0).getEquality()).isEqualTo(EQUAL);
        assertThat(composites[0].getComponent(0).getValue()).isEqualTo(1);
        assertThat(composites[0].getComponent(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);
        assertThat(composites[0].getComponent(1).getValue()).isEqualTo("a");

        assertThat(composites[1].getComponent(0).getEquality()).isEqualTo(EQUAL);
        assertThat(composites[1].getComponent(0).getValue()).isEqualTo(5);
        assertThat(composites[1].getComponent(1).getEquality()).isEqualTo(EQUAL);
        assertThat(composites[1].getComponent(1).getValue()).isEqualTo("c");
        assertThat(composites[1].getComponent(2).getEquality()).isEqualTo(GREATER_THAN_EQUAL);
        assertThat(composites[1].getComponent(2).getValue()).isEqualTo(uuid);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_create_composite_key_for_counter() throws Exception {
        PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);

        when(idMeta.writeValueToString(11L)).thenReturn("11");
        Composite comp = factory.createKeyForCounter("fqcn", 11L, idMeta);

        assertThat(comp.getComponents()).hasSize(2);
        assertThat(comp.getComponent(0).getValue()).isEqualTo("fqcn");
        assertThat(comp.getComponent(1).getValue()).isEqualTo("11");

    }
}
