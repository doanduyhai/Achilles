package info.archinnov.achilles.iterator.factory;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.proxy.wrapper.ThriftCounterWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.Pair;
import java.util.Arrays;
import java.util.List;
import mapping.entity.TweetCompoundKey;
import mapping.entity.UserBean;
import me.prettyprint.cassandra.model.HCounterColumnImpl;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import testBuilders.CompositeTestBuilder;
import testBuilders.HColumnTestBuilder;
import testBuilders.PropertyMetaTestBuilder;
import com.google.common.collect.Lists;

/**
 * ThriftCompositeTransformerTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftCompositeTransformerTest
{
    @InjectMocks
    private ThriftCompositeTransformer transformer;

    @Mock
    private ThriftCompoundKeyMapper compoundKeyMapper;

    @Mock
    private ThriftEntityProxifier proxifier;

    @Mock
    private ThriftPersistenceContext context;

    @Mock
    private ThriftPersistenceContext joinContext;

    private ObjectMapper mapper = new ObjectMapper();

    @Before
    public void setUp()
    {
        Whitebox.setInternalState(transformer, "compoundKeyMapper", compoundKeyMapper);
    }

    @Test
    public void should_build_single_key_transformer() throws Exception
    {
        Composite comp1 = CompositeTestBuilder.builder().values(45).buildSimple();
        Composite comp2 = CompositeTestBuilder.builder().values(51).buildSimple();
        HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
        HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

        PropertyMeta<Integer, String> propertyMeta = PropertyMetaTestBuilder //
                .noClass(Integer.class, String.class)
                .type(WIDE_MAP)
                .consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ALL, ALL))
                .build();

        List<Integer> keys = Lists.transform(Arrays.asList(hCol1, hCol2),
                transformer.buildKeyTransformer(propertyMeta));

        assertThat(keys).containsExactly(45, 51);
    }

    @Test
    public void should_build_multi_key_transformer() throws Exception
    {
        Composite comp1 = CompositeTestBuilder.builder().values("a", "b").buildSimple();
        Composite comp2 = CompositeTestBuilder.builder().values("c", "d").buildSimple();
        HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
        HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

        PropertyMeta<TweetCompoundKey, String> propertyMeta = PropertyMetaTestBuilder.noClass(
                TweetCompoundKey.class, String.class).build();

        TweetCompoundKey multiKey1 = new TweetCompoundKey();
        TweetCompoundKey multiKey2 = new TweetCompoundKey();

        when(compoundKeyMapper.readFromComposite(propertyMeta, hCol1.getName().getComponents()))
                .thenReturn(multiKey1);
        when(compoundKeyMapper.readFromComposite(propertyMeta, hCol2.getName().getComponents()))
                .thenReturn(multiKey2);
        List<TweetCompoundKey> keys = Lists.transform(Arrays.asList(hCol1, hCol2),
                transformer.buildKeyTransformer(propertyMeta));

        assertThat(keys).containsExactly(multiKey1, multiKey2);
    }

    @Test
    public void should_build_value_transformer() throws Exception
    {
        Composite comp1 = CompositeTestBuilder.builder().buildSimple();
        Composite comp2 = CompositeTestBuilder.builder().buildSimple();
        HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
        HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

        PropertyMeta<Integer, String> propertyMeta = PropertyMetaTestBuilder
                .noClass(Integer.class, String.class)
                .type(WIDE_MAP)
                .build();

        List<String> values = Lists.transform(Arrays.asList(hCol1, hCol2),
                transformer.buildValueTransformer(propertyMeta));

        assertThat(values).containsExactly("test1", "test2");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_build_raw_value_transformer() throws Exception
    {
        Composite comp1 = CompositeTestBuilder.builder().buildSimple();
        Composite comp2 = CompositeTestBuilder.builder().buildSimple();
        HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
        HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

        List<Object> rawValues = Lists.transform(Arrays.asList(hCol1, hCol2),
                transformer.buildRawValueTransformer());

        assertThat(rawValues).containsExactly("test1", "test2");
    }

    @Test
    public void should_build_join_value_from_composite() throws Exception
    {
        EntityMeta joinMeta = new EntityMeta();
        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, String.class)
                .field("user")
                .joinMeta(joinMeta)
                .type(PropertyType.JOIN_SIMPLE)
                .build();

        UserBean user = new UserBean();
        Composite comp = new Composite();

        String userAsString = mapper.writeValueAsString(user);
        HColumn<Composite, String> hColumn = HColumnTestBuilder.simple(comp, userAsString);

        when(context.newPersistenceContext(joinMeta, hColumn.getValue())).thenReturn(joinContext);
        when(proxifier.buildProxy(hColumn.getValue(), joinContext)).thenReturn(userAsString);
        String actual = transformer.buildValue(context, propertyMeta, hColumn);

        assertThat(actual).isSameAs(userAsString);
    }

    @Test
    public void should_build_ttl_transformer() throws Exception
    {
        Composite comp1 = CompositeTestBuilder.builder().buildSimple();
        Composite comp2 = CompositeTestBuilder.builder().buildSimple();
        HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1", 12);
        HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2", 13);

        List<Integer> rawValues = Lists.transform(Arrays.asList(hCol1, hCol2),
                transformer.buildTtlTransformer());

        assertThat(rawValues).containsExactly(12, 13);
    }

    @Test
    public void should_build_timestamp_transformer() throws Exception
    {
        Composite comp1 = CompositeTestBuilder.builder().buildSimple();
        Composite comp2 = CompositeTestBuilder.builder().buildSimple();
        HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1", 12);
        HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2", 13);

        hCol1.setClock(10);
        hCol2.setClock(11);

        List<Long> rawValues = Lists.transform(Arrays.asList(hCol1, hCol2),
                transformer.buildTimestampTransformer());

        assertThat(rawValues).containsExactly(10L, 11L);
    }

    @Test
    public void should_build_key_value_transformer() throws Exception
    {
        Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
        Composite comp2 = CompositeTestBuilder.builder().values(12).buildSimple();
        HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1", 456);
        HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2", 789);

        PropertyMeta<Integer, String> propertyMeta = PropertyMetaTestBuilder
                .noClass(Integer.class, String.class)
                .type(WIDE_MAP)
                .consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ALL, ALL))
                .build();

        List<KeyValue<Integer, String>> keyValues = Lists.transform(Arrays.asList(hCol1, hCol2),
                transformer.buildKeyValueTransformer(context, propertyMeta));

        assertThat(keyValues).hasSize(2);

        assertThat(keyValues.get(0).getKey()).isEqualTo(11);
        assertThat(keyValues.get(0).getValue()).isEqualTo("test1");
        assertThat(keyValues.get(0).getTtl()).isEqualTo(456);

        assertThat(keyValues.get(1).getKey()).isEqualTo(12);
        assertThat(keyValues.get(1).getValue()).isEqualTo("test2");
        assertThat(keyValues.get(1).getTtl()).isEqualTo(789);
    }

    @Test
    public void should_counter_key_value_transformer() throws Exception {
        Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
        Composite comp2 = CompositeTestBuilder.builder().values(12).buildSimple();

        HCounterColumn<Composite> hCol1 = new HCounterColumnImpl<Composite>(comp1, 11L);
        HCounterColumn<Composite> hCol2 = new HCounterColumnImpl<Composite>(comp2, 12L);

        PropertyMeta<Integer, Counter> propertyMeta = PropertyMetaTestBuilder
                .noClass(Integer.class, Counter.class)
                .type(WIDE_MAP)
                .consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ALL, ALL))
                .build();

        List<KeyValue<Integer, Counter>> keyValues = Lists.
                transform(Arrays.asList(hCol1, hCol2),
                        transformer.buildCounterKeyValueTransformer(context, propertyMeta));

        assertThat(keyValues).hasSize(2);

        assertThat(keyValues.get(0).getKey()).isEqualTo(11);
        assertThat(keyValues.get(0).getValue()).isInstanceOf(ThriftCounterWrapper.class);

        assertThat(keyValues.get(1).getKey()).isEqualTo(12);
        assertThat(keyValues.get(1).getValue()).isInstanceOf(ThriftCounterWrapper.class);

        List<Integer> keys = Lists.transform(Arrays.asList(hCol1, hCol2),
                transformer.buildCounterKeyTransformer(propertyMeta));

        assertThat(keys).hasSize(2);

        assertThat(keys.get(0)).isEqualTo(11);
        assertThat(keys.get(1)).isEqualTo(12);

        List<Counter> values = Lists.transform(Arrays.asList(hCol1, hCol2),
                transformer.buildCounterValueTransformer(context, propertyMeta));

        assertThat(values).hasSize(2);
        assertThat(values.get(0)).isInstanceOf(ThriftCounterWrapper.class);
        assertThat(values.get(1)).isInstanceOf(ThriftCounterWrapper.class);
    }

    @Test
    public void should_build_compound_counter_key() throws Exception {
        Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
        Composite comp2 = CompositeTestBuilder.builder().values(12).buildSimple();

        HCounterColumn<Composite> hCol1 = new HCounterColumnImpl<Composite>(comp1, 11L);
        HCounterColumn<Composite> hCol2 = new HCounterColumnImpl<Composite>(comp2, 12L);

    }

}
