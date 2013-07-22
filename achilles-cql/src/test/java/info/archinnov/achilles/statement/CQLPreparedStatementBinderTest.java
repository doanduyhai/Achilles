package info.archinnov.achilles.statement;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
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
    private ReflectionInvoker invoker;

    @Mock
    private PreparedStatement ps;

    @Mock
    private BoundStatement bs;

    @Mock
    private DataTranscoder transcoder;

    @Mock
    private ObjectMapper objectMapper;

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
        long primaryKey = RandomUtils.nextLong();
        long age = RandomUtils.nextLong();
        String name = "name";

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.ID)
                .transcoder(transcoder)
                .build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .accessors()
                .transcoder(transcoder)
                .build();

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("age")
                .type(PropertyType.SIMPLE)
                .accessors()
                .transcoder(transcoder)
                .build();

        PropertyMeta<?, ?> wideMapMeta = PropertyMetaTestBuilder
                .completeBean(UUID.class, String.class)
                .field("geoPositions")
                .type(PropertyType.WIDE_MAP)
                .accessors()
                .build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setPropertyMetas(ImmutableMap.of
                ("id", idMeta, name, nameMeta, "age", ageMeta, "wideMap", wideMapMeta));

        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn(name);
        when(invoker.getValueFromField(entity, ageMeta.getGetter())).thenReturn(age);

        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(transcoder.encode(nameMeta, name)).thenReturn(name);
        when(transcoder.encode(ageMeta, age)).thenReturn(age);

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
        assertThat(boundValues).containsExactly(primaryKey, name, age);

    }

    @Test
    public void should_bind_for_insert_with_join_entity() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.ID)
                .transcoder(transcoder)
                .build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .accessors()
                .transcoder(transcoder)
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
                .transcoder(transcoder)
                .build();

        UserBean user = new UserBean();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setPropertyMetas(ImmutableMap.of
                ("id", idMeta, "name", nameMeta, "user", userMeta));

        long primaryKey = RandomUtils.nextLong();
        long joinId = RandomUtils.nextLong();
        String name = "name";

        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn(name);
        when(invoker.getValueFromField(entity, userMeta.getGetter())).thenReturn(user);

        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(transcoder.encode(nameMeta, name)).thenReturn(name);
        when(transcoder.encode(userMeta, user)).thenReturn(joinId);

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
        assertThat(boundValues).containsExactly(primaryKey, name, joinId);

    }

    @Test
    public void should_bind_for_insert_with_null_fields() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.ID)
                .transcoder(transcoder)
                .build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(PropertyType.SIMPLE)
                .accessors()
                .transcoder(transcoder)
                .build();

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("age")
                .type(PropertyType.SIMPLE)
                .accessors()
                .transcoder(transcoder)
                .build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setPropertyMetas(ImmutableMap.of
                ("id", idMeta, "name", nameMeta, "age", ageMeta));

        long primaryKey = RandomUtils.nextLong();
        String name = "name";
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn(name);
        when(invoker.getValueFromField(entity, ageMeta.getGetter())).thenReturn(null);

        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(transcoder.encode(nameMeta, name)).thenReturn(name);
        when(transcoder.encode(eq(ageMeta), any())).thenReturn(null);

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
        assertThat(boundValues).containsExactly(primaryKey, name, null);

    }

    @Test
    public void should_bind_for_insert_with_compound_key() throws Exception
    {
        long userId = RandomUtils.nextLong();
        long age = RandomUtils.nextLong();
        String name = "name";

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(PropertyType.EMBEDDED_ID)
                .transcoder(transcoder)
                .build();

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("age")
                .type(PropertyType.SIMPLE)
                .transcoder(transcoder)
                .accessors()
                .build();

        entityMeta.setIdMeta(idMeta);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("age", ageMeta));

        CompoundKey compoundKey = new CompoundKey(userId, name);

        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(compoundKey);
        when(invoker.getValueFromField(entity, ageMeta.getGetter())).thenReturn(age);

        when(transcoder.encodeToComponents(idMeta, compoundKey)).thenReturn((List) Arrays.asList(userId, name));
        when(transcoder.encode(ageMeta, age)).thenReturn(age);

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
        assertThat(boundValues).containsExactly(userId, name, age);
    }

    @Test
    public void should_bind_with_only_pk_in_where_clause() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(ID)
                .transcoder(transcoder)
                .build();
        entityMeta.setIdMeta(idMeta);
        long primaryKey = RandomUtils.nextLong();

        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);

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

        BoundStatement actual = binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, primaryKey);

        assertThat(actual).isSameAs(bs);
        assertThat(boundValues).containsExactly(primaryKey);
    }

    @Test
    public void should_bind_for_update() throws Exception
    {
        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .accessors()
                .type(ID)
                .transcoder(transcoder)
                .build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .accessors()
                .type(SIMPLE)
                .transcoder(transcoder)
                .build();

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("age")
                .accessors()
                .type(SIMPLE)
                .transcoder(transcoder)
                .build();

        entityMeta.setIdMeta(idMeta);

        long primaryKey = RandomUtils.nextLong();
        long age = RandomUtils.nextLong();
        String name = "name";

        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(primaryKey);
        when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn(name);
        when(invoker.getValueFromField(entity, ageMeta.getGetter())).thenReturn(age);

        when(transcoder.encode(idMeta, primaryKey)).thenReturn(primaryKey);
        when(transcoder.encode(nameMeta, name)).thenReturn(name);
        when(transcoder.encode(ageMeta, age)).thenReturn(age);

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
        assertThat(boundValues).containsExactly(name, age, primaryKey);
    }

    @Test
    public void should_bind_for_simple_counter_increment_decrement() throws Exception
    {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .transcoder(transcoder)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        PropertyMeta<Void, Long> counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("counter")
                .transcoder(transcoder)
                .build();

        Long primaryKey = RandomUtils.nextLong();
        Long counter = RandomUtils.nextLong();

        when(transcoder.forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
        when(transcoder.forceEncodeToJSON(counter)).thenReturn(counter.toString());
        when(ps.bind(counter, "CompleteBean", primaryKey.toString(), "counter")).thenReturn(bs);

        assertThat(binder.bindForSimpleCounterIncrementDecrement(ps, meta, counterMeta, primaryKey, counter))
                .isSameAs(bs);
    }

    @Test
    public void should_bind_for_simple_counter_select() throws Exception
    {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .transcoder(transcoder)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        PropertyMeta<Void, Long> counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("counter")
                .transcoder(transcoder)
                .build();

        Long primaryKey = RandomUtils.nextLong();

        when(transcoder.forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
        when(ps.bind("CompleteBean", primaryKey.toString(), "counter")).thenReturn(bs);

        assertThat(binder.bindForSimpleCounterSelect(ps, meta, counterMeta, primaryKey)).isSameAs(bs);
    }

    @Test
    public void should_bind_for_simple_counter_delete() throws Exception
    {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .transcoder(transcoder)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setClassName("CompleteBean");
        meta.setIdMeta(idMeta);

        PropertyMeta<Void, Long> counterMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("counter")
                .transcoder(transcoder)
                .build();

        Long primaryKey = RandomUtils.nextLong();

        when(transcoder.forceEncodeToJSON(primaryKey)).thenReturn(primaryKey.toString());
        when(ps.bind("CompleteBean", primaryKey.toString(), "counter")).thenReturn(bs);

        assertThat(binder.bindForSimpleCounterDelete(ps, meta, counterMeta, primaryKey)).isSameAs(bs);
    }
}
