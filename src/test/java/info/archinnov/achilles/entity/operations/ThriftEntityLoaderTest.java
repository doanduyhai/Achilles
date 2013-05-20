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
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.impl.ThriftJoinLoaderImpl;
import info.archinnov.achilles.entity.operations.impl.ThriftLoaderImpl;
import info.archinnov.achilles.exception.AchillesException;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import testBuilders.PropertyMetaTestBuilder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

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
	public void setUp() throws Exception
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
	public void should_not_load_entity() throws Exception
	{
		Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);
		when((PropertyMeta<Void, Long>) entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getSetter()).thenReturn(idSetter);

		when(entityMeta.isWideRow()).thenReturn(false);
		when(loaderImpl.load(context, CompleteBean.class)).thenReturn(bean);
		context.setLoadEagerFields(false);

		Object actual = loader.load(context, CompleteBean.class);

		assertThat(actual).isNotSameAs(bean);

		verify(introspector).setValueToField(any(CompleteBean.class), eq(idSetter),
				eq(bean.getId()));
		verifyZeroInteractions(loaderImpl);

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

	@Test
	public void should_load_version_serial_UID() throws Exception
	{
		when(loaderImpl.loadVersionSerialUID(bean.getId(), dao)).thenReturn(123L);

		Long actual = loader.loadVersionSerialUID(bean.getId(), dao);
		assertThat(actual).isEqualTo(123L);
	}

	@Test
	public void should_load_simple() throws Exception
	{
		String value = "val";
		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.SIMPLE);
		when(loaderImpl.loadSimpleProperty(context, propertyMeta)).thenReturn(value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_load_lazy_simple() throws Exception
	{
		String value = "val";
		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.LAZY_SIMPLE);
		when(loaderImpl.loadSimpleProperty(context, propertyMeta)).thenReturn(value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_load_list() throws Exception
	{
		List<String> value = Arrays.asList("val");
		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.LIST);
		when(loaderImpl.loadListProperty(context, propertyMeta)).thenReturn(value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_load_lazy_list() throws Exception
	{
		List<String> value = Arrays.asList("val");
		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.LAZY_LIST);
		when(loaderImpl.loadListProperty(context, propertyMeta)).thenReturn(value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_load_set() throws Exception
	{
		Set<String> value = Sets.newHashSet("val");
		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.SET);
		when(loaderImpl.loadSetProperty(context, propertyMeta)).thenReturn(value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_load_lazy_set() throws Exception
	{
		Set<String> value = Sets.newHashSet("val");
		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.LAZY_SET);
		when(loaderImpl.loadSetProperty(context, propertyMeta)).thenReturn(value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_load_map() throws Exception
	{
		Map<Integer, String> value = ImmutableMap.of(11, "val");

		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.MAP);
		when((Map<Integer, String>) loaderImpl.loadMapProperty(context, propertyMeta)).thenReturn(
				value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_load_lazy_map() throws Exception
	{
		Map<Integer, String> value = ImmutableMap.of(11, "val");

		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.LAZY_MAP);
		when((Map<Integer, String>) loaderImpl.loadMapProperty(context, propertyMeta)).thenReturn(
				value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_load_join_simple() throws Exception
	{
		String value = "val";

		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_SIMPLE);
		when(loaderImpl.loadJoinSimple(context, propertyMeta, loader)).thenReturn(value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_load_join_list() throws Exception
	{
		List<String> value = Arrays.asList("val");

		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_LIST);
		when(joinLoaderImpl.loadJoinListProperty(context, propertyMeta)).thenReturn(value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_load_join_set() throws Exception
	{
		Set<String> value = Sets.newHashSet("val");

		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_SET);
		when(joinLoaderImpl.loadJoinSetProperty(context, propertyMeta)).thenReturn(value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_load_join_map() throws Exception
	{
		Map<Integer, String> value = ImmutableMap.of(11, "val");

		Method setter = prepareSetter();
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_MAP);
		when((Map<Integer, String>) joinLoaderImpl.loadJoinMapProperty(context, propertyMeta))
				.thenReturn(value);

		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verify(introspector).setValueToField(bean, setter, value);
	}

	@Test
	public void should_not_load() throws Exception
	{
		when(propertyMeta.type()).thenReturn(PropertyType.COUNTER);
		Method setter = prepareSetter();
		loader.loadPropertyIntoObject(bean, bean.getId(), context, propertyMeta);

		verifyZeroInteractions(loaderImpl, joinLoaderImpl, introspector);
	}

	private Method prepareSetter() throws Exception
	{
		PropertyMeta<Void, String> tempMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.accessors()
				.build();

		when(propertyMeta.getSetter()).thenReturn(tempMeta.getSetter());

		return tempMeta.getSetter();
	}
}
