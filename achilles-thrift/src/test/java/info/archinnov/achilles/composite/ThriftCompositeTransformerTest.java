package info.archinnov.achilles.composite;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.test.builders.CompositeTestBuilder;
import info.archinnov.achilles.test.builders.HColumnTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
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
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private ThriftCompositeTransformer transformer;

    @Mock
    private ThriftCompoundKeyMapper compoundKeyMapper;

    @Mock
    private ThriftEntityProxifier proxifier;

    @Mock
    private ThriftEntityMapper entityMapper;

    @Mock
    private ThriftPersistenceContext context;

    @Mock
    private ThriftPersistenceContext joinContext;

    private ObjectMapper mapper = new ObjectMapper();

    @Before
    public void setUp()
    {
        Whitebox.setInternalState(transformer, ThriftCompoundKeyMapper.class, compoundKeyMapper);
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
    public void should_build_clustered_entity_transformer() throws Exception
    {
        Object partitionKey = RandomUtils.nextLong();
        BeanWithClusteredId expected = new BeanWithClusteredId();
        Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
        String clusteredValue = "value";
        HColumn<Composite, Object> hCol1 = HColumnTestBuilder
                .<Object> simple(comp1, clusteredValue);

        PropertyMeta<CompoundKey, CompoundKey> idMeta = PropertyMetaTestBuilder
                .keyValueClass(CompoundKey.class, CompoundKey.class).type(EMBEDDED_ID).build();

        PropertyMeta<Void, String> pm = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(SIMPLE)
                .accessors()
                .build();

        when(context.getIdMeta()).thenReturn((PropertyMeta) idMeta);
        when(context.getFirstMeta()).thenReturn((PropertyMeta) pm);

        CompoundKey compoundKey = new CompoundKey();
        when(context.getPartitionKey()).thenReturn(partitionKey);
        when(
                compoundKeyMapper.fromCompositeToEmbeddedId(eq(idMeta), any(List.class),
                        eq(partitionKey))).thenReturn(
                compoundKey);

        when(entityMapper.createClusteredEntityWithValue(BeanWithClusteredId.class, idMeta,
                pm, compoundKey, clusteredValue)).thenReturn(expected);

        Function<HColumn<Composite, Object>, BeanWithClusteredId> function = transformer
                .buildClusteredEntityTransformer(BeanWithClusteredId.class, context);

        List<BeanWithClusteredId> actualList = Lists.transform(Arrays.asList(hCol1), function);

        assertThat(actualList).hasSize(1);
        BeanWithClusteredId actual = actualList.get(0);
        assertThat(actual).isSameAs(expected);

    }

    @Test
    public void should_build_counter_clustered_entity_transformer() throws Exception
    {
        Object partitionKey = RandomUtils.nextLong();
        BeanWithClusteredId expected = new BeanWithClusteredId();
        Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
        HCounterColumn<Composite> hCol1 = HColumnTestBuilder.counter(comp1, 150L);

        PropertyMeta<CompoundKey, CompoundKey> idMeta = PropertyMetaTestBuilder
                .keyValueClass(CompoundKey.class, CompoundKey.class).type(EMBEDDED_ID).build();

        PropertyMeta<Void, Long> pm = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("count")
                .type(PropertyType.COUNTER)
                .accessors()
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("name", pm));

        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getIdMeta()).thenReturn((PropertyMeta) idMeta);

        CompoundKey compoundKey = new CompoundKey();
        when(context.getPartitionKey()).thenReturn(partitionKey);
        when(compoundKeyMapper.fromCompositeToEmbeddedId(eq(idMeta), any(List.class),
                eq(partitionKey))).thenReturn(compoundKey);

        when(entityMapper.initClusteredEntity(BeanWithClusteredId.class, idMeta, compoundKey))
                .thenReturn(expected);

        Function<HCounterColumn<Composite>, BeanWithClusteredId> function = transformer
                .buildCounterClusteredEntityTransformer(BeanWithClusteredId.class, context);

        List<BeanWithClusteredId> actualList = Lists.transform(Arrays.asList(hCol1), function);

        assertThat(actualList).hasSize(1);
        BeanWithClusteredId actual = actualList.get(0);
        assertThat(actual).isSameAs(expected);
    }

    @Test
    public void should_build_join_clustered_entity_transformer() throws Exception
    {
        Object partitionKey = RandomUtils.nextLong();

        BeanWithClusteredId expected = new BeanWithClusteredId();
        long joinId = 10L;
        UserBean joinEntity = new UserBean();
        Map<Object, Object> joinEntitiesMap = ImmutableMap.<Object, Object> of(joinId,
                joinEntity);

        Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
        HColumn<Composite, Object> hCol1 = HColumnTestBuilder
                .<Object> simple(comp1, joinId);

        PropertyMeta<CompoundKey, CompoundKey> idMeta = PropertyMetaTestBuilder
                .keyValueClass(CompoundKey.class, CompoundKey.class).type(EMBEDDED_ID).build();

        PropertyMeta<Void, UserBean> pm = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_SIMPLE)
                .accessors()
                .build();

        when(context.getIdMeta()).thenReturn((PropertyMeta) idMeta);
        when(context.getFirstMeta()).thenReturn((PropertyMeta) pm);

        CompoundKey embeddedId = new CompoundKey();
        when(context.getPartitionKey()).thenReturn(partitionKey);
        when(compoundKeyMapper.fromCompositeToEmbeddedId(eq(idMeta), any(List.class),
                eq(partitionKey))).thenReturn(embeddedId);

        when(entityMapper.createClusteredEntityWithValue(eq(BeanWithClusteredId.class),
                eq(idMeta), eq(pm), eq(embeddedId), any(UserBean.class))).thenReturn(expected);

        Function<HColumn<Composite, Object>, BeanWithClusteredId> function = transformer
                .buildJoinClusteredEntityTransformer(BeanWithClusteredId.class, context,
                        joinEntitiesMap);

        List<BeanWithClusteredId> actualList = Lists.transform(Arrays.asList(hCol1), function);

        assertThat(actualList).hasSize(1);
        BeanWithClusteredId actual = actualList.get(0);

        assertThat(actual).isSameAs(expected);
    }

}
