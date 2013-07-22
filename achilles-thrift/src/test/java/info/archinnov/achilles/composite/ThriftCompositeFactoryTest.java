package info.archinnov.achilles.composite;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static info.archinnov.achilles.type.BoundingMode.*;
import static info.archinnov.achilles.type.OrderingMode.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.compound.ThriftCompoundKeyValidator;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.query.ThriftQueryValidator;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.TweetCompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;
import java.util.Arrays;
import java.util.List;
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
 * ThriftCompositeFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftCompositeFactoryTest
{
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @InjectMocks
    private ThriftCompositeFactory factory;

    @Mock
    private ComponentEqualityCalculator calculator;

    @Mock
    private ThriftCompoundKeyMapper compoundKeyMapper;

    @Mock
    private ThriftQueryValidator queryValidator;

    @Mock
    private ThriftCompoundKeyValidator compoundKeyValidator;

    @Mock
    private PropertyMeta<Integer, String> wideMapMeta;

    @Mock
    private PropertyMeta<TweetCompoundKey, String> compoundKeyWideMapMeta;

    @Before
    public void setUp()
    {
        when(wideMapMeta.isCompound()).thenReturn(false);
        when(wideMapMeta.getPropertyName()).thenReturn("property");
        when(wideMapMeta.getKeyClass()).thenReturn(Integer.class);

        when(compoundKeyWideMapMeta.isCompound()).thenReturn(true);
        when(compoundKeyWideMapMeta.getPropertyName()).thenReturn("property");
    }

    @Test
    public void should_create_for_insert() throws Exception
    {
        Composite comp = factory.createBaseComposite(wideMapMeta, 12);

        assertThat(comp.getComponents()).hasSize(1);
        assertThat((Integer) comp.getComponents().get(0).getValue()).isEqualTo(12);
    }

    @Test
    public void should_create_for_compound_key_insert() throws Exception
    {
        TweetCompoundKey tweetKey = new TweetCompoundKey();
        Composite comp = new Composite();

        when(
                compoundKeyMapper.fromCompoundToCompositeForInsertOrGet(tweetKey,
                        compoundKeyWideMapMeta))
                .thenReturn(comp);

        Composite actual = factory.createBaseComposite(compoundKeyWideMapMeta, tweetKey);

        assertThat(actual).isSameAs(comp);
    }

    @Test
    public void should_create_for_query() throws Exception
    {
        Composite comp = factory.createForQuery(wideMapMeta, 123, LESS_THAN_EQUAL);

        assertThat(comp.getComponents()).hasSize(1);
        assertThat((Integer) comp.getComponents().get(0).getValue()).isEqualTo(123);
        assertThat(comp.getComponents().get(0).getEquality()).isEqualTo(LESS_THAN_EQUAL);
    }

    @Test
    public void should_create_null_for_query() throws Exception
    {
        Composite comp = factory.createForQuery(wideMapMeta, null, LESS_THAN_EQUAL);
        assertThat(comp).isNull();
    }

    @Test
    public void should_create_composites_for_query() throws Exception
    {

        when(
                calculator.determineEquality(INCLUSIVE_START_BOUND_ONLY,
                        OrderingMode.ASCENDING)) //
                .thenReturn(new ComponentEquality[]
                {
                        EQUAL,
                        LESS_THAN_EQUAL
                });
        Composite[] composites = factory.createForQuery(wideMapMeta, 12, 15,
                INCLUSIVE_START_BOUND_ONLY, ASCENDING);

        assertThat(composites).hasSize(2);
        assertThat(composites[0].getComponent(0).getEquality()).isEqualTo(EQUAL);
        assertThat(composites[0].getComponent(0).getValue()).isEqualTo(12);
        assertThat(composites[1].getComponent(0).getEquality()).isEqualTo(LESS_THAN_EQUAL);
        assertThat(composites[1].getComponent(0).getValue()).isEqualTo(15);

    }

    @Test
    public void should_create_compound_key_composites_for_query() throws Exception
    {
        TweetCompoundKey tweetKey1 = new TweetCompoundKey();
        TweetCompoundKey tweetKey2 = new TweetCompoundKey();
        Composite comp1 = new Composite();
        Composite comp2 = new Composite();

        when(
                calculator.determineEquality(BoundingMode.INCLUSIVE_END_BOUND_ONLY,
                        OrderingMode.ASCENDING)) //
                .thenReturn(new ComponentEquality[]
                {
                        LESS_THAN_EQUAL,
                        GREATER_THAN_EQUAL
                });

        when(
                compoundKeyMapper.fromCompoundToCompositeForQuery(tweetKey1,
                        compoundKeyWideMapMeta,
                        LESS_THAN_EQUAL))
                .thenReturn(comp1);
        when(
                compoundKeyMapper.fromCompoundToCompositeForQuery(tweetKey2,
                        compoundKeyWideMapMeta,
                        GREATER_THAN_EQUAL))
                .thenReturn(comp2);

        Composite[] composites = factory.createForQuery(
                compoundKeyWideMapMeta, tweetKey1, tweetKey2,
                BoundingMode.INCLUSIVE_END_BOUND_ONLY,
                OrderingMode.ASCENDING);

        assertThat(composites).hasSize(2);
        assertThat(composites[0]).isSameAs(comp1);
        assertThat(composites[1]).isSameAs(comp2);
    }

    @Test
    public void should_create_key_for_counter() throws Exception
    {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder.valueClass(Long.class).build();

        Composite comp = factory.createKeyForCounter("fqcn", 11L, idMeta);

        assertThat(comp.getComponents()).hasSize(2);
        assertThat(comp.getComponent(0).getValue(STRING_SRZ)).isEqualTo("fqcn");
        assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("11");
    }

    @Test
    public void should_create_base_for_get() throws Exception
    {
        PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .type(SIMPLE)
                .field("name")
                .build();

        Composite comp = factory.createBaseForGet(meta);

        assertThat(comp.getComponents()).hasSize(3);
        assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(SIMPLE.flag());
        assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
        assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
        assertThat(comp.getComponent(1).getEquality()).isEqualTo(EQUAL);
        assertThat(comp.getComponent(2).getValue(INT_SRZ)).isEqualTo(0);
        assertThat(comp.getComponent(2).getEquality()).isEqualTo(EQUAL);
    }

    @Test
    public void should_create_base_for_clustered_get() throws Exception
    {
        Object compoundKey = new CompoundKey();
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .type(SIMPLE)
                .build();

        Composite comp = new Composite();
        when(compoundKeyMapper.fromCompoundToCompositeForInsertOrGet(compoundKey, idMeta))
                .thenReturn(comp);
        Composite actual = factory.createBaseForClusteredGet(compoundKey, idMeta);

        assertThat(actual).isSameAs(comp);
    }

    @Test
    public void should_create_base_for_counter_get() throws Exception
    {
        PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .type(SIMPLE)
                .field("name")
                .build();

        Composite comp = factory.createBaseForCounterGet(meta);

        assertThat(comp.getComponents()).hasSize(1);
        assertThat(comp.getComponent(0).getValue(STRING_SRZ)).isEqualTo("name");
        assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
    }

    @Test
    public void should_create_base_for_query() throws Exception
    {
        PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .type(SIMPLE)
                .field("name")
                .build();

        Composite comp = factory.createBaseForQuery(meta, GREATER_THAN_EQUAL);

        assertThat(comp.getComponents()).hasSize(2);
        assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(SIMPLE.flag());
        assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
        assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
        assertThat(comp.getComponent(1).getEquality()).isEqualTo(GREATER_THAN_EQUAL);
    }

    @Test
    public void should_create_for_batch_insert_single() throws Exception
    {
        PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .type(SIMPLE)
                .field("name")
                .build();

        Composite comp = factory.createForBatchInsertSingleValue(meta);

        assertThat(comp.getComponents()).hasSize(3);
        assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(SIMPLE.flag());
        assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
        assertThat(comp.getComponent(2).getValue(INT_SRZ)).isEqualTo(0);
    }

    @Test
    public void should_create_for_batch_insert_single_counter() throws Exception
    {
        PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .type(SIMPLE)
                .field("name")
                .build();

        Composite comp = factory.createForBatchInsertSingleCounter(meta);

        assertThat(comp.getComponents()).hasSize(1);
        assertThat(comp.getComponent(0).getValue(STRING_SRZ)).isEqualTo("name");
    }

    @Test
    public void should_create_for_batch_insert_multiple() throws Exception
    {
        PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .type(SIMPLE)
                .field("name")
                .build();

        Composite comp = factory.createForBatchInsertMultiValue(meta, 21);

        assertThat(comp.getComponents()).hasSize(3);
        assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(SIMPLE.flag());
        assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
        assertThat(comp.getComponent(2).getValue(INT_SRZ)).isEqualTo(21);
    }

    @Test
    public void should_create_for_clustered_query() throws Exception
    {

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
                .valueClass(Long.class)
                .type(SIMPLE)
                .field("name")
                .compClasses(Long.class, String.class)
                .build();

        List<Object> clusteringFrom = Arrays.<Object> asList(11L, "z");
        List<Object> clusteringTo = Arrays.<Object> asList(11L, "a");
        Composite from = new Composite(), to = new Composite();

        when(calculator.determineEquality(EXCLUSIVE_BOUNDS, DESCENDING)).thenReturn(
                new ComponentEquality[]
                {
                        LESS_THAN_EQUAL,
                        GREATER_THAN_EQUAL
                });

        when(
                compoundKeyMapper.fromComponentsToCompositeForQuery(clusteringFrom, pm,
                        LESS_THAN_EQUAL)).thenReturn(
                from);
        when(
                compoundKeyMapper.fromComponentsToCompositeForQuery(clusteringTo, pm,
                        GREATER_THAN_EQUAL))
                .thenReturn(to);

        Composite[] composites = factory.createForClusteredQuery(pm, clusteringFrom,
                clusteringTo,
                EXCLUSIVE_BOUNDS,
                DESCENDING);

        assertThat(composites[0]).isSameAs(from);
        assertThat(composites[1]).isSameAs(to);

        verify(compoundKeyValidator).validateCompoundKeysForClusteredQuery(pm, clusteringFrom,
                clusteringTo,
                DESCENDING);

    }
}
