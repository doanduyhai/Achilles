package info.archinnov.achilles.entity.operations.impl;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftImmediateFlushContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContextTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.helper.ThriftEntityMapper;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

/**
 * ThriftLoaderImplTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftLoaderImplTest
{

	@InjectMocks
	private ThriftLoaderImpl loaderImpl;

	@Mock
	private ThriftEntityLoader loader;

	@Mock
	private ThriftEntityMapper mapper;

	@Mock
	private AchillesMethodInvoker invoker;

	@Mock
	private ThriftCompositeFactory thriftCompositeFactory;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private ThriftImmediateFlushContext thriftImmediateFlushContext;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	@Captor
	ArgumentCaptor<CompleteBean> beanCaptor;

	@Captor
	ArgumentCaptor<ThriftPersistenceContext> contextCaptor;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private ObjectMapper objectMapper = new ObjectMapper();

	private PropertyMeta<Void, Long> idMeta;

	private ThriftPersistenceContext context;

	@Before
	public void setUp() throws Throwable
	{
		idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class)
				.field("id")
				.accessors()
				.build();

		context = ThriftPersistenceContextTestBuilder
				.context(entityMeta, thriftCounterDao, policy, CompleteBean.class, entity.getId())
				.entity(entity)
				.thriftImmediateFlushContext(thriftImmediateFlushContext)
				.entityDao(entityDao)
				.build();
	}

	@Test
	public void should_load() throws Exception
	{

		Composite comp = new Composite();
		List<Pair<Composite, String>> values = new ArrayList<Pair<Composite, String>>();
		values.add(new Pair<Composite, String>(comp, "value"));

		when(entityDao.eagerFetchEntity(entity.getId())).thenReturn(values);
		when((PropertyMeta<Void, Long>) entityMeta.getIdMeta()).thenReturn(idMeta);

		CompleteBean actual = loaderImpl.load(context, CompleteBean.class);

		verify(mapper).setEagerPropertiesToEntity(eq(entity.getId()), eq(values), eq(entityMeta),
				beanCaptor.capture());
		verify(invoker).setValueToField(beanCaptor.capture(), eq(idMeta.getSetter()),
				eq(entity.getId()));

		assertThat(beanCaptor.getAllValues()).containsExactly(actual, actual);
	}

	@Test
	public void should_load_version_serial_uuid() throws Exception
	{
		when(entityDao.getValue(eq(entity.getId()), any(Composite.class))).thenReturn("123");
		Long actual = loaderImpl.loadVersionSerialUID(entity.getId(), entityDao);
		assertThat(actual).isEqualTo(123L);
	}

	@Test
	public void should_load_simple_property() throws Exception
	{

		PropertyMeta<Void, String> nameMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class)
				.field("name")
				.accessors()
				.build();

		Composite comp = new Composite();
		when(thriftCompositeFactory.createBaseForGet(nameMeta)).thenReturn(comp);
		when(entityDao.getValue(entity.getId(), comp)).thenReturn("name_xyz");

		String actual = loaderImpl.loadSimpleProperty(context, nameMeta);
		assertThat(actual).isEqualTo("name_xyz");
	}

	@Test
	public void should_load_list() throws Exception
	{
		PropertyMeta<Void, String> listMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class)
				.field("friends")
				.accessors()
				.build();

		Composite start = new Composite(), end = new Composite();
		List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();
		columns.add(new Pair<Composite, Object>(start, "foo"));
		columns.add(new Pair<Composite, Object>(end, "bar"));

		when(thriftCompositeFactory.createBaseForQuery(listMeta, EQUAL)).thenReturn(start);
		when(thriftCompositeFactory.createBaseForQuery(listMeta, GREATER_THAN_EQUAL)).thenReturn(
				end);
		when(entityDao.findColumnsRange(entity.getId(), start, end, false, Integer.MAX_VALUE))
				.thenReturn(columns);

		List<String> actual = loaderImpl.loadListProperty(context, listMeta);
		assertThat(actual).containsExactly("foo", "bar");
	}

	@Test
	public void should_load_set() throws Exception
	{
		PropertyMeta<Void, String> setMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class)
				.field("followers")
				.accessors()
				.build();

		Composite start = new Composite(), end = new Composite();
		List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();
		columns.add(new Pair<Composite, Object>(start, "John"));
		columns.add(new Pair<Composite, Object>(end, "Helen"));

		when(thriftCompositeFactory.createBaseForQuery(setMeta, EQUAL)).thenReturn(start);
		when(thriftCompositeFactory.createBaseForQuery(setMeta, GREATER_THAN_EQUAL))
				.thenReturn(end);
		when(entityDao.findColumnsRange(entity.getId(), start, end, false, Integer.MAX_VALUE))
				.thenReturn(columns);

		Set<String> actual = loaderImpl.loadSetProperty(context, setMeta);
		assertThat(actual).containsExactly("John", "Helen");
	}

	@Test
	public void should_load_map() throws Exception
	{
		PropertyMeta<Integer, UserBean> setMeta = PropertyMetaTestBuilder //
				.completeBean(Integer.class, UserBean.class)
				.field("usersMap")
				.type(PropertyType.MAP)
				.accessors()
				.build();

		Composite start = new Composite(), end = new Composite();
		List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();

		UserBean user1 = new UserBean(), user2 = new UserBean();
		user1.setName("user1");
		user2.setName("user2");

		columns.add(new Pair<Composite, Object>(start,
				writeToString(new KeyValue<Integer, UserBean>(1, user1))));
		columns.add(new Pair<Composite, Object>(end, writeToString(new KeyValue<Integer, UserBean>(
				2, user2))));

		when(thriftCompositeFactory.createBaseForQuery(setMeta, EQUAL)).thenReturn(start);
		when(thriftCompositeFactory.createBaseForQuery(setMeta, GREATER_THAN_EQUAL))
				.thenReturn(end);
		when(entityDao.findColumnsRange(entity.getId(), start, end, false, Integer.MAX_VALUE))
				.thenReturn(columns);

		Map<Integer, UserBean> actual = loaderImpl.loadMapProperty(context, setMeta);
		assertThat(actual).hasSize(2);
		assertThat(actual.get(1).getName()).isEqualTo("user1");
		assertThat(actual.get(2).getName()).isEqualTo("user2");
	}

	@Test
	public void should_load_join_simple() throws Exception
	{
		String stringJoinId = RandomUtils.nextLong() + "";
		EntityMeta joinMeta = new EntityMeta();
		joinMeta.setIdMeta(idMeta);

		PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Integer.class, UserBean.class)
				.field("user")
				.joinMeta(joinMeta)
				.type(PropertyType.JOIN_SIMPLE)
				.accessors()
				.build();

		UserBean user = new UserBean();
		Composite comp = new Composite();
		when(thriftCompositeFactory.createBaseForGet(propertyMeta)).thenReturn(comp);
		when(entityDao.getValue(entity.getId(), comp)).thenReturn(stringJoinId);
		when(loader.load(contextCaptor.capture(), eq(UserBean.class))).thenReturn(user);

		UserBean actual = loaderImpl.loadJoinSimple(context, propertyMeta, loader);
		assertThat(actual).isSameAs(user);
		assertThat(contextCaptor.getValue().getPrimaryKey()).isEqualTo(new Long(stringJoinId));
		assertThat(contextCaptor.getValue().getEntityMeta()).isSameAs(joinMeta);
	}

	@Test
	public void should_return_null_when_no_join_entity() throws Exception
	{
		EntityMeta joinMeta = new EntityMeta();
		joinMeta.setIdMeta(idMeta);

		PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Integer.class, UserBean.class)
				.field("user")
				.joinMeta(joinMeta)
				.type(PropertyType.JOIN_SIMPLE)
				.accessors()
				.build();

		Composite comp = new Composite();
		when(thriftCompositeFactory.createBaseForGet(propertyMeta)).thenReturn(comp);
		when(entityDao.getValue(entity.getId(), comp)).thenReturn(null);

		UserBean actual = loaderImpl.loadJoinSimple(context, propertyMeta, loader);
		assertThat(actual).isNull();

	}

	private String writeToString(KeyValue<Integer, UserBean> keyValue) throws Exception
	{
		return objectMapper.writeValueAsString(keyValue);
	}
}
