package info.archinnov.achilles.statement;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.compound.CQLCompoundKeyMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.MethodInvoker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import parser.entity.CompoundKey;
import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.collect.ImmutableMap;

/**
 * CQLPreparedStatementBinderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLPreparedStatementBinderTest
{
    @InjectMocks
    private CQLPreparedStatementBinder binder;

    @Mock
    private MethodInvoker invoker;

    @Mock
    private CQLCompoundKeyMapper mapper;

    @Mock
    private PreparedStatement ps;

    @Mock
    private BoundStatement bs;

    private List<Object> boundValues = new ArrayList<Object>();

    private EntityMeta entityMeta;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    @Before
    public void setUp()
    {
        entityMeta = new EntityMeta();
        boundValues.clear();
    }

    @Test
    public void should_bind_for_insert_with_simple_id() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.ID)
                .build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .accessors()
                .build();

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("age")
                .type(PropertyType.SIMPLE)
                .accessors()
                .build();

        PropertyMeta<?, ?> wideMapMeta = PropertyMetaTestBuilder
                .completeBean(UUID.class, String.class)
                .field("geoPositions")
                .type(PropertyType.WIDE_MAP)
                .accessors()
                .build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setPropertyMetas(ImmutableMap.of
                ("id", idMeta, "name", nameMeta, "age", ageMeta, "wideMap", wideMapMeta));

        when(invoker.getValueFromField(entity, idMeta.getGetter())).thenReturn(11L);
        when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn("name");
        when(invoker.getValueFromField(entity, ageMeta.getGetter())).thenReturn(30L);

        when(ps.bind(Matchers.<Object> anyVararg())).thenAnswer(new Answer<BoundStatement>()
        {
            @Override
            public BoundStatement answer(InvocationOnMock invocation) throws Throwable
            {
                for (Object value : invocation.getArguments())
                {
                    boundValues.add(value);
                }
                return bs;
            }
        });

        BoundStatement actual = binder.bindForInsert(ps, entityMeta, entity);

        assertThat(actual).isSameAs(bs);
        assertThat(boundValues).containsExactly(11L, "name", 30L);

    }

    @Test
    public void should_bind_for_insert_with_join_entity() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.SIMPLE)
                .build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .accessors()
                .build();

        PropertyMeta<Void, Long> joinIdMeta = new PropertyMeta<Void, Long>();
        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(joinIdMeta);

        PropertyMeta<?, ?> userMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, UserBean.class)
                .field("user")
                .type(PropertyType.JOIN_SIMPLE)
                .joinMeta(joinMeta)
                .accessors()
                .build();

        UserBean user = new UserBean();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setPropertyMetas(ImmutableMap.of
                ("id", idMeta, "name", nameMeta, "user", userMeta));

        when(invoker.getValueFromField(entity, idMeta.getGetter())).thenReturn(11L);
        when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn("name");
        when(invoker.getValueFromField(entity, userMeta.getGetter())).thenReturn(user);
        when(invoker.getPrimaryKey(user, joinIdMeta)).thenReturn(123L);

        when(ps.bind(Matchers.<Object> anyVararg())).thenAnswer(new Answer<BoundStatement>()
        {
            @Override
            public BoundStatement answer(InvocationOnMock invocation) throws Throwable
            {
                for (Object value : invocation.getArguments())
                {
                    boundValues.add(value);
                }
                return bs;
            }
        });

        BoundStatement actual = binder.bindForInsert(ps, entityMeta, entity);

        assertThat(actual).isSameAs(bs);
        assertThat(boundValues).containsExactly(11L, "name", 123L);

    }

    @Test
    public void should_bind_for_insert_with_null_fields() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.SIMPLE)
                .build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .accessors()
                .build();

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("age")
                .type(PropertyType.SIMPLE)
                .accessors()
                .build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setPropertyMetas(ImmutableMap.of
                ("id", idMeta, "name", nameMeta, "age", ageMeta));

        when(invoker.getValueFromField(entity, idMeta.getGetter())).thenReturn(11L);
        when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn("name");
        when(invoker.getValueFromField(entity, ageMeta.getGetter())).thenReturn(null);

        when(ps.bind(Matchers.<Object> anyVararg())).thenAnswer(new Answer<BoundStatement>()
        {
            @Override
            public BoundStatement answer(InvocationOnMock invocation) throws Throwable
            {
                for (Object value : invocation.getArguments())
                {
                    boundValues.add(value);
                }
                return bs;
            }
        });

        BoundStatement actual = binder.bindForInsert(ps, entityMeta, entity);

        assertThat(actual).isSameAs(bs);
        assertThat(boundValues).containsExactly(11L, "name", null);

    }

    @Test
    public void should_bind_for_insert_with_compound_key() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.COMPOUND_ID)
                .build();

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("age")
                .type(PropertyType.SIMPLE)
                .accessors()
                .build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("age", ageMeta));

        CompoundKey compoundKey = new CompoundKey(11L, "name");

        when(mapper.extractComponents(compoundKey, idMeta)).thenReturn(Arrays.<Object> asList(11L, "name"));

        when(invoker.getValueFromField(entity, idMeta.getGetter())).thenReturn(compoundKey);
        when(invoker.getValueFromField(entity, ageMeta.getGetter())).thenReturn(30L);

        when(ps.bind(Matchers.<Object> anyVararg())).thenAnswer(new Answer<BoundStatement>()
        {
            @Override
            public BoundStatement answer(InvocationOnMock invocation) throws Throwable
            {
                for (Object value : invocation.getArguments())
                {
                    boundValues.add(value);
                }
                return bs;
            }
        });

        BoundStatement actual = binder.bindForInsert(ps, entityMeta, entity);

        assertThat(actual).isSameAs(bs);
        assertThat(boundValues).containsExactly(11L, "name", 30L);
    }

    @Test
    public void should_bind_with_only_pk_in_where_clause() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.SIMPLE)
                .build();
        entityMeta.setIdMeta(idMeta);

        when(ps.bind(Matchers.<Object> anyVararg())).thenAnswer(new Answer<BoundStatement>()
        {
            @Override
            public BoundStatement answer(InvocationOnMock invocation) throws Throwable
            {
                for (Object value : invocation.getArguments())
                {
                    boundValues.add(value);
                }
                return bs;
            }
        });

        BoundStatement actual = binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, 11L);

        assertThat(actual).isSameAs(bs);
        assertThat(boundValues).containsExactly(11L);
    }

    @Test
    public void should_bind_for_update() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.SIMPLE)
                .build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .accessors()
                .build();

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("age")
                .type(PropertyType.SIMPLE)
                .accessors()
                .build();

        entityMeta.setIdMeta(idMeta);

        when(invoker.getValueFromField(entity, idMeta.getGetter())).thenReturn(11L);
        when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn("name");
        when(invoker.getValueFromField(entity, ageMeta.getGetter())).thenReturn(30L);

        when(ps.bind(Matchers.<Object> anyVararg())).thenAnswer(new Answer<BoundStatement>()
        {
            @Override
            public BoundStatement answer(InvocationOnMock invocation) throws Throwable
            {
                for (Object value : invocation.getArguments())
                {
                    boundValues.add(value);
                }
                return bs;
            }
        });

        BoundStatement actual = binder.bindForUpdate(ps, entityMeta,
                Arrays.asList(nameMeta, ageMeta), entity);

        assertThat(actual).isSameAs(bs);
        assertThat(boundValues).containsExactly("name", 30L, 11L);
    }

    @Test
    public void should_bind_for_simple_counter_increment_decrement() throws Exception
    {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        PropertyMeta<Void, Long> counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("counter")
                .build();

        when(ps.bind(2L, "CompleteBean", "11", "counter")).thenReturn(bs);

        assertThat(binder.bindForSimpleCounterIncrementDecrement(ps, meta, counterMeta, 11L, 2L))
                .isSameAs(bs);
    }

    @Test
    public void should_bind_for_simple_counter_select() throws Exception
    {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        PropertyMeta<Void, Long> counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("counter")
                .build();

        when(ps.bind("CompleteBean", "11", "counter")).thenReturn(bs);

        assertThat(binder.bindForSimpleCounterSelect(ps, meta, counterMeta, 11L)).isSameAs(bs);
    }

    @Test
    public void should_bind_for_simple_counter_delete() throws Exception
    {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        PropertyMeta<Void, Long> counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("counter")
                .build();

        when(ps.bind("CompleteBean", "11", "counter")).thenReturn(bs);

        assertThat(binder.bindForSimpleCounterDelete(ps, meta, counterMeta, 11L)).isSameAs(bs);
    }
}
