package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.AchillesEntityIntrospector;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.ThriftJoinLoaderImpl;
import info.archinnov.achilles.entity.operations.impl.ThriftLoaderImpl;
import info.archinnov.achilles.exception.AchillesException;

import java.lang.reflect.Method;

import mapping.entity.CompleteBean;
import me.prettyprint.cassandra.model.ExecutingKeyspace;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * EntityLoaderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityLoaderTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftEntityLoader loader;

	@Mock
	private ExecutingKeyspace keyspace;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@Mock
	private PropertyMeta<?, String> propertyMeta;

	@Mock
	private PropertyMeta<Void, String> listMeta;

	@Mock
	private PropertyMeta<Void, String> setMeta;

	@Mock
	private PropertyMeta<Integer, String> mapMeta;

	@Mock
	private EntityMeta joinMeta;

	@Mock
	private PropertyMeta<Void, Long> joinIdMeta;

	@Mock
	private ThriftEntityMapper mapper;

	@Mock
	private ThriftGenericEntityDao dao;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private CompositeFactory compositeFactory;

	@Mock
	private AchillesEntityIntrospector introspector;

	@Mock
	private ThriftJoinLoaderImpl joinLoaderImpl;

	@Mock
	private ThriftLoaderImpl loaderImpl;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	@Captor
	ArgumentCaptor<Long> idCaptor;

	private CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().buid();

	private ThriftPersistenceContext context;

	@Before
	public void setUp()
	{
		context = PersistenceContextTestBuilder
				.context(entityMeta, thriftCounterDao, policy, CompleteBean.class, bean.getId())
				.entity(bean)
				.build();
	}

	@Test
	public void should_load_entity() throws Exception
	{
		when(entityMeta.isWideRow()).thenReturn(false);
		when(loaderImpl.load(context, CompleteBean.class)).thenReturn(bean);

		Object actual = loader.load(context, CompleteBean.class);

		assertThat(actual).isSameAs(bean);
	}

	@Test
	public void should_load_wide_row() throws Exception
	{
		Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);
		when(entityMeta.isWideRow()).thenReturn(true);
		when((PropertyMeta<Void, Long>) entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getSetter()).thenReturn(idSetter);

		Object actual = loader.load(context, CompleteBean.class);

		assertThat(actual).isNotNull();
		verify(introspector).setValueToField(any(CompleteBean.class), eq(idSetter),
				eq(bean.getId()));
		verifyZeroInteractions(loaderImpl);
	}

	@Test
	public void should_throw_exception_on_load_error() throws Exception
	{
		when(entityMeta.isWideRow()).thenReturn(false);
		when(loaderImpl.load(context, CompleteBean.class)).thenThrow(new RuntimeException("test"));

		exception.expect(AchillesException.class);
		exception.expectMessage("Error when loading entity type '"
				+ CompleteBean.class.getCanonicalName() + "' with key '" + bean.getId()
				+ "'. Cause : test");

		loader.load(context, CompleteBean.class);
	}
}
