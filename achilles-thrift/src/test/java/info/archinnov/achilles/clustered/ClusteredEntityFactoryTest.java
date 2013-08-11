package info.archinnov.achilles.clustered;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.ThriftJoinEntityLoader;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class ClusteredEntityFactoryTest
{

    @InjectMocks
    private ClusteredEntityFactory factory;

    @Mock
    private ThriftCompositeTransformer transformer;

    @Mock
    private ThriftJoinEntityLoader joinHelper;

    @Mock
    private ThriftPersistenceContext context;

    @Mock
    private HColumn<Composite, Object> hCol;

    @Mock
    private HCounterColumn<Composite> hCounterCol;

    private List<HColumn<Composite, Object>> hColumns;
    private List<HCounterColumn<Composite>> hCounterColumns;

    private EntityMeta meta;

    private PropertyMeta idMeta;

    private BeanWithClusteredId entity = new BeanWithClusteredId();

    @Before
    public void setUp() throws Exception
    {

        idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class)
                .field("id")
                .type(PropertyType.EMBEDDED_ID)
                .accessors()
                .build();

        meta = new EntityMeta();
        meta.setIdMeta(idMeta);

        when(context.getEntityMeta()).thenReturn(meta);
        hColumns = Arrays.asList(hCol);
        hCounterColumns = Arrays.asList(hCounterCol);
    }

    @Test
    public void should_return_empty_list_when_empty_hcolumns() throws Exception
    {
        PropertyMeta pm = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .build();
        when(context.getFirstMeta()).thenReturn(pm);
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));

        List<BeanWithClusteredId> actual = factory.buildClusteredEntities(
                BeanWithClusteredId.class, context, new ArrayList<HColumn<Composite, Object>>());

        assertThat(actual).isEmpty();
    }

    @Test
    public void should_build_simple_clustered_entity() throws Exception
    {

        PropertyMeta pm = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .build();
        when(context.getFirstMeta()).thenReturn(pm);
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));

        when(context.getFirstMeta()).thenReturn(pm);
        when(transformer.clusteredEntityTransformer(BeanWithClusteredId.class, context))
                .thenReturn(
                        new Function<HColumn<Composite, Object>, BeanWithClusteredId>()
                        {
                            @Override
                            public BeanWithClusteredId apply(HColumn<Composite, Object> hCol)
                            {
                                return entity;
                            }
                        });
        List<BeanWithClusteredId> clusteredEntities = factory.buildClusteredEntities(
                BeanWithClusteredId.class,
                context, hColumns);

        assertThat(clusteredEntities).containsExactly(entity);
    }

    @Test
    public void should_build_counter_clustered_entity() throws Exception
    {

        PropertyMeta pm = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("count")
                .type(PropertyType.COUNTER)
                .build();
        when(context.getFirstMeta()).thenReturn(pm);
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));

        when(transformer.counterClusteredEntityTransformer(BeanWithClusteredId.class, context))
                .thenReturn(
                        new Function<HCounterColumn<Composite>, BeanWithClusteredId>()
                        {
                            @Override
                            public BeanWithClusteredId apply(HCounterColumn<Composite> hCol)
                            {
                                return entity;
                            }
                        });
        List<BeanWithClusteredId> clusteredEntities = factory.buildCounterClusteredEntities(
                BeanWithClusteredId.class, context, hCounterColumns);

        assertThat(clusteredEntities).containsExactly(entity);
    }

    @Test
    public void should_build_join_clustered_entity() throws Exception
    {
        UserBean user = new UserBean();
        Map<Object, Object> joinEntitiesMap = ImmutableMap.<Object, Object> of(10L, user);
        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setTableName("joinTable");
        PropertyMeta pm = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_SIMPLE)
                .joinMeta(joinMeta)
                .build();

        when(context.getFirstMeta()).thenReturn(pm);
        meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "pm", pm));

        when(transformer.buildRawValueTransformer()).thenReturn(
                (Function) new Function<HColumn<Composite, Object>, Object>()
                {
                    @Override
                    public Object apply(HColumn<Composite, Object> hCol)
                    {
                        return 10L;
                    }
                });

        ThriftGenericEntityDao entityDao = mock(ThriftGenericEntityDao.class);
        when(context.findEntityDao("joinTable")).thenReturn(entityDao);
        when(
                joinHelper.loadJoinEntities(eq(UserBean.class), any(List.class), eq(joinMeta),
                        eq(entityDao)))
                .thenReturn(joinEntitiesMap);

        when(
                transformer.joinClusteredEntityTransformer(BeanWithClusteredId.class, context,
                        joinEntitiesMap))
                .thenReturn(new Function<HColumn<Composite, Object>, BeanWithClusteredId>()
                {
                    @Override
                    public BeanWithClusteredId apply(HColumn<Composite, Object> hCol)
                    {
                        return entity;
                    }
                });
        List<BeanWithClusteredId> clusteredEntities = factory.buildClusteredEntities(
                BeanWithClusteredId.class,
                context, hColumns);

        assertThat(clusteredEntities).containsExactly(entity);
    }

    @Test
    public void should_build_value_less_clustered_entity() throws Exception
    {
        when(context.isValueless()).thenReturn(true);
        when(transformer.valuelessClusteredEntityTransformer(BeanWithClusteredId.class, context))
                .thenReturn(
                        new Function<HColumn<Composite, Object>, BeanWithClusteredId>()
                        {
                            @Override
                            public BeanWithClusteredId apply(HColumn<Composite, Object> hCol)
                            {
                                return entity;
                            }
                        });
        List<BeanWithClusteredId> clusteredEntities = factory.buildClusteredEntities(
                BeanWithClusteredId.class,
                context, hColumns);

        assertThat(clusteredEntities).containsExactly(entity);
    }
}
