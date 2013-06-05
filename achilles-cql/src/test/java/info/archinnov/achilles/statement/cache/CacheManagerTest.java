package info.archinnov.achilles.statement.cache;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.statement.CQLPreparedStatementGenerator;

import java.util.Arrays;
import java.util.List;

import mapping.entity.CompleteBean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.cache.Cache;

/**
 * CacheManagerTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CacheManagerTest
{
	@InjectMocks
	private CacheManager manager;

	@Mock
	private CQLPreparedStatementGenerator generator;

	@Mock
	private Session session;

	@Mock
	private Cache<StatementCacheKey, PreparedStatement> cache;

	@Mock
	private CQLPersistenceContext context;

	@Mock
	private PreparedStatement ps;

	@Captor
	ArgumentCaptor<StatementCacheKey> cacheKeyCaptor;

	@Test
	public void should_get_cache_for_simple_field() throws Exception
	{
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");

		PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
				.valueClass(String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityMeta()).thenReturn(meta);
		when(cache.getIfPresent(cacheKeyCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = manager.getCacheForFieldSelect(session, cache, context, pm);

		assertThat(actual).isSameAs(ps);
		StatementCacheKey cacheKey = cacheKeyCaptor.getValue();
		assertThat((Class) cacheKey.getEntityClass()).isSameAs(CompleteBean.class);
		assertThat(cacheKey.getTableName()).isEqualTo("table");
		assertThat(cacheKey.getType()).isEqualTo(CacheType.SELECT_FIELD);
		assertThat(cacheKey.getFields()).containsExactly("name");
	}

	@Test
	public void should_get_cache_for_clustered_id() throws Exception
	{
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");

		PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
				.valueClass(String.class)
				.field("name")
				.compNames("id", "a", "b")
				.type(PropertyType.CLUSTERED_KEY)
				.build();

		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityMeta()).thenReturn(meta);
		when(cache.getIfPresent(cacheKeyCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = manager.getCacheForFieldSelect(session, cache, context, pm);

		assertThat(actual).isSameAs(ps);
		StatementCacheKey cacheKey = cacheKeyCaptor.getValue();
		assertThat(cacheKey.getFields()).containsOnly("id", "a", "b");
	}

	@Test
	public void should_generate_select_prepared_statement_when_not_found_in_cache()
			throws Exception
	{
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");

		PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
				.valueClass(String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityMeta()).thenReturn(meta);
		when(cache.getIfPresent(cacheKeyCaptor.capture())).thenReturn(null);
		when(generator.prepareSelectFieldPS(session, meta, pm)).thenReturn(ps);

		PreparedStatement actual = manager.getCacheForFieldSelect(session, cache, context, pm);

		assertThat(actual).isSameAs(ps);
		StatementCacheKey cacheKey = cacheKeyCaptor.getValue();
		verify(cache).put(cacheKey, ps);
	}

	@Test
	public void should_get_cache_for_fields_update() throws Exception
	{
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("age")
				.type(PropertyType.SIMPLE)
				.build();

		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityMeta()).thenReturn(meta);
		when(cache.getIfPresent(cacheKeyCaptor.capture())).thenReturn(ps);

		PreparedStatement actual = manager.getCacheForFieldsUpdate(session, cache, context,
				Arrays.asList(nameMeta, ageMeta));

		assertThat(actual).isSameAs(ps);
		StatementCacheKey cacheKey = cacheKeyCaptor.getValue();
		assertThat((Class) cacheKey.getEntityClass()).isSameAs(CompleteBean.class);
		assertThat(cacheKey.getTableName()).isEqualTo("table");
		assertThat(cacheKey.getType()).isEqualTo(CacheType.UPDATE_FIELDS);
		assertThat(cacheKey.getFields()).containsOnly("name", "age");
	}

	@Test
	public void should_generate_update_prepared_statement_when_not_found_in_cache()
			throws Exception
	{
		EntityMeta meta = new EntityMeta();
		meta.setTableName("table");

		PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.type(PropertyType.SIMPLE)
				.build();

		PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("age")
				.type(PropertyType.SIMPLE)
				.build();

		List<PropertyMeta<?, ?>> pms = Arrays.asList(nameMeta, ageMeta);

		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityMeta()).thenReturn(meta);
		when(cache.getIfPresent(cacheKeyCaptor.capture())).thenReturn(null);
		when(generator.prepareUpdateFields(session, meta, pms)).thenReturn(ps);

		PreparedStatement actual = manager.getCacheForFieldsUpdate(session, cache, context, pms);

		assertThat(actual).isSameAs(ps);
		StatementCacheKey cacheKey = cacheKeyCaptor.getValue();
		assertThat((Class) cacheKey.getEntityClass()).isSameAs(CompleteBean.class);
		assertThat(cacheKey.getTableName()).isEqualTo("table");
		assertThat(cacheKey.getType()).isEqualTo(CacheType.UPDATE_FIELDS);
		assertThat(cacheKey.getFields()).containsOnly("name", "age");
	}
}
