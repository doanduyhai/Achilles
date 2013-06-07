package info.archinnov.achilles.statement;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.MethodInvoker;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.ClusteredId;
import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

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
	private PreparedStatement ps;

	@Mock
	private BoundStatement bs;

	@Captor
	private ArgumentCaptor<List<Object>> valuesCaptor;

	private EntityMeta entityMeta;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Before
	public void setUp()
	{
		entityMeta = new EntityMeta();
	}

	@Test
	public void should_bind_for_insert_with_simple_id() throws Exception
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
				.accessors()
				.build();

		PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("age")
				.accessors()
				.build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetas(Arrays.asList(nameMeta, ageMeta));

		when(invoker.getValueFromField(entity, idMeta.getGetter())).thenReturn(11L);
		when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn("name");
		when(invoker.getValueFromField(entity, ageMeta.getGetter())).thenReturn(30L);

		when(ps.bind(valuesCaptor.capture())).thenReturn(bs);

		BoundStatement actual = binder.bindForInsert(ps, entityMeta, entity);

		assertThat(actual).isSameAs(bs);
		assertThat(valuesCaptor.getValue()).containsExactly(11L, "name", 30L);

	}

	@Test
	public void should_bind_for_insert_with_clustered_id() throws Exception
	{
		Method userIdGetter = ClusteredId.class.getDeclaredMethod("getUserId");
		Method nameGetter = ClusteredId.class.getDeclaredMethod("getName");
		PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.accessors()
				.type(PropertyType.CLUSTERED_KEY)
				.compGetters(userIdGetter, nameGetter)
				.build();

		PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("age")
				.accessors()
				.build();

		entityMeta.setIdMeta(idMeta);
		entityMeta.setAllMetas(Arrays.<PropertyMeta<?, ?>> asList(ageMeta));

		when(invoker.getValueFromField(entity, userIdGetter)).thenReturn(11L);
		when(invoker.getValueFromField(entity, nameGetter)).thenReturn("name");
		when(invoker.getValueFromField(entity, ageMeta.getGetter())).thenReturn(30L);

		when(ps.bind(valuesCaptor.capture())).thenReturn(bs);

		BoundStatement actual = binder.bindForInsert(ps, entityMeta, entity);

		assertThat(actual).isSameAs(bs);
		assertThat(valuesCaptor.getValue()).containsExactly(11L, "name", 30L);
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

		when(invoker.getValueFromField(entity, idMeta.getGetter())).thenReturn(11L);

		when(ps.bind(valuesCaptor.capture())).thenReturn(bs);

		BoundStatement actual = binder.bindStatementWithOnlyPKInWhereClause(ps, entityMeta, entity);

		assertThat(actual).isSameAs(bs);
		assertThat(valuesCaptor.getValue()).containsExactly(11L);
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
				.accessors()
				.build();

		PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("age")
				.accessors()
				.build();

		entityMeta.setIdMeta(idMeta);

		when(invoker.getValueFromField(entity, idMeta.getGetter())).thenReturn(11L);
		when(invoker.getValueFromField(entity, nameMeta.getGetter())).thenReturn("name");
		when(invoker.getValueFromField(entity, ageMeta.getGetter())).thenReturn(30L);

		when(ps.bind(valuesCaptor.capture())).thenReturn(bs);

		BoundStatement actual = binder.bindForUpdate(ps, entityMeta,
				Arrays.asList(nameMeta, ageMeta), entity);

		assertThat(actual).isSameAs(bs);
		assertThat(valuesCaptor.getValue()).containsExactly("name", 30L, 11L);

	}
}
