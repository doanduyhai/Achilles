package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static javax.persistence.CascadeType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.columnFamily.AchillesTableCreator;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.context.AchillesConfigurationContext;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parser.context.EntityParsingContext;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.json.ObjectMapperFactory;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.CascadeType;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.Bean;
import parser.entity.BeanWithColumnFamilyName;
import parser.entity.BeanWithDuplicatedColumnName;
import parser.entity.BeanWithDuplicatedJoinColumnName;
import parser.entity.BeanWithExternalJoinWideMap;
import parser.entity.BeanWithExternalWideMap;
import parser.entity.BeanWithJoinColumnAsWideMap;
import parser.entity.BeanWithNoColumn;
import parser.entity.BeanWithNoId;
import parser.entity.BeanWithNotSerializableId;
import parser.entity.BeanWithSimpleCounter;
import parser.entity.BeanWithWideMapCounter;
import parser.entity.ChildBean;
import parser.entity.UserBean;
import parser.entity.WideRowBean;
import parser.entity.WideRowBeanWithJoinEntity;
import parser.entity.WideRowBeanWithTwoColumns;
import parser.entity.WideRowBeanWithWrongColumnType;

@RunWith(MockitoJUnitRunner.class)
public class EntityParserTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private EntityParser parser;

	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	@Mock
	private AchillesTableCreator thriftTableCreator;

	@Mock
	private Map<Class<?>, EntityMeta> entityMetaMap;

	private AchillesConfigurationContext configContext = new AchillesConfigurationContext();

	private ObjectMapperFactory objectMapperFactory = new ObjectMapperFactory()
	{
		@Override
		public <T> ObjectMapper getMapper(Class<T> type)
		{
			return objectMapper;
		}

	};
	private ObjectMapper objectMapper = new ObjectMapper();

	private EntityParsingContext entityContext;

	@Before
	public void setUp()
	{
		joinPropertyMetaToBeFilled.clear();
		configContext.setConsistencyPolicy(policy);
		configContext.setObjectMapperFactory(objectMapperFactory);

		when(policy.getDefaultGlobalReadConsistencyLevel()).thenReturn(ConsistencyLevel.ONE);
		when(policy.getDefaultGlobalWriteConsistencyLevel()).thenReturn(ConsistencyLevel.ALL);
	}

	@Test
	public void should_parse_entity() throws Exception
	{

		initEntityParsingContext(Bean.class);
		EntityMeta meta = parser.parseEntity(entityContext);

		assertThat(meta.getClassName()).isEqualTo("parser.entity.Bean");
		assertThat(meta.getTableName()).isEqualTo("Bean");
		assertThat(meta.getSerialVersionUID()).isEqualTo(1L);
		assertThat((Class<Long>) meta.getIdMeta().getValueClass()).isEqualTo(Long.class);
		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat((Class<Long>) meta.getIdClass()).isEqualTo(Long.class);
		assertThat(meta.getPropertyMetas()).hasSize(7);

		PropertyMeta<?, ?> name = meta.getPropertyMetas().get("name");
		PropertyMeta<?, ?> age = meta.getPropertyMetas().get("age_in_year");
		PropertyMeta<Void, String> friends = (PropertyMeta<Void, String>) meta
				.getPropertyMetas()
				.get("friends");
		PropertyMeta<Void, String> followers = (PropertyMeta<Void, String>) meta
				.getPropertyMetas()
				.get("followers");
		PropertyMeta<Integer, String> preferences = (PropertyMeta<Integer, String>) meta
				.getPropertyMetas()
				.get("preferences");

		PropertyMeta<Void, UserBean> creator = (PropertyMeta<Void, UserBean>) meta
				.getPropertyMetas()
				.get("creator");
		PropertyMeta<String, UserBean> linkedUsers = (PropertyMeta<String, UserBean>) meta
				.getPropertyMetas()
				.get("linked_users");

		assertThat(name).isNotNull();
		assertThat(age).isNotNull();
		assertThat(friends).isNotNull();
		assertThat(followers).isNotNull();
		assertThat(preferences).isNotNull();
		assertThat(creator).isNotNull();
		assertThat(linkedUsers).isNotNull();

		assertThat(name.getPropertyName()).isEqualTo("name");
		assertThat((Class<String>) name.getValueClass()).isEqualTo(String.class);
		assertThat(name.type()).isEqualTo(SIMPLE);
		assertThat(name.getReadConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
		assertThat(name.getWriteConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);

		assertThat(age.getPropertyName()).isEqualTo("age_in_year");
		assertThat((Class<Long>) age.getValueClass()).isEqualTo(Long.class);
		assertThat(age.type()).isEqualTo(SIMPLE);
		assertThat(age.getReadConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
		assertThat(age.getWriteConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);

		assertThat(friends.getPropertyName()).isEqualTo("friends");
		assertThat(friends.getValueClass()).isEqualTo(String.class);
		assertThat(friends.type()).isEqualTo(PropertyType.LAZY_LIST);
		assertThat(friends.type().isLazy()).isTrue();
		assertThat(friends.getReadConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
		assertThat(friends.getWriteConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);

		assertThat(followers.getPropertyName()).isEqualTo("followers");
		assertThat(followers.getValueClass()).isEqualTo(String.class);
		assertThat(followers.type()).isEqualTo(PropertyType.SET);
		assertThat(followers.getReadConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
		assertThat(followers.getWriteConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);

		assertThat(preferences.getPropertyName()).isEqualTo("preferences");
		assertThat(preferences.getValueClass()).isEqualTo(String.class);
		assertThat(preferences.type()).isEqualTo(PropertyType.MAP);
		assertThat(preferences.getKeyClass()).isEqualTo(Integer.class);
		assertThat(preferences.getReadConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
		assertThat(preferences.getWriteConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);

		assertThat(creator.getPropertyName()).isEqualTo("creator");
		assertThat(creator.getValueClass()).isEqualTo(UserBean.class);
		assertThat(creator.type()).isEqualTo(JOIN_SIMPLE);
		assertThat(creator.getJoinProperties().getCascadeTypes()).containsExactly(CascadeType.ALL);

		assertThat(linkedUsers.getPropertyName()).isEqualTo("linked_users");
		assertThat(linkedUsers.getValueClass()).isEqualTo(UserBean.class);
		assertThat(linkedUsers.type()).isEqualTo(JOIN_WIDE_MAP);
		assertThat(linkedUsers.getJoinProperties().getCascadeTypes()).contains(PERSIST, MERGE);

		assertThat((Class) joinPropertyMetaToBeFilled.get(creator)).isEqualTo(UserBean.class);
		assertThat((Class) joinPropertyMetaToBeFilled.get(linkedUsers)).isEqualTo(UserBean.class);

		assertThat(meta.getConsistencyLevels().left).isEqualTo(ConsistencyLevel.ONE);
		assertThat(meta.getConsistencyLevels().right).isEqualTo(ConsistencyLevel.ALL);

		assertThat(meta.getEagerMetas()).containsOnly(name, age, followers, preferences);
		assertThat(meta.getEagerGetters()).containsOnly(name.getGetter(), age.getGetter(),
				followers.getGetter(), preferences.getGetter());

		verify(policy).setConsistencyLevelForRead(ConsistencyLevel.ONE, meta.getTableName());
		verify(policy).setConsistencyLevelForWrite(ConsistencyLevel.ALL, meta.getTableName());
	}

	@Test
	public void should_parse_entity_with_table_name() throws Exception
	{

		initEntityParsingContext(BeanWithColumnFamilyName.class);

		EntityMeta meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		assertThat(meta.getTableName()).isEqualTo("myOwnCF");
	}

	@Test
	public void should_parse_inherited_bean() throws Exception
	{
		initEntityParsingContext(ChildBean.class);
		EntityMeta meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getPropertyMetas().get("name").getPropertyName()).isEqualTo("name");
		assertThat(meta.getPropertyMetas().get("address").getPropertyName()).isEqualTo("address");
		assertThat(meta.getPropertyMetas().get("nickname").getPropertyName()).isEqualTo("nickname");
	}

	@Test
	public void should_parse_bean_with_simple_counter_field() throws Exception
	{
		initEntityParsingContext(BeanWithSimpleCounter.class);
		EntityMeta meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		assertThat(entityContext.getHasSimpleCounter()).isTrue();
		PropertyMeta<Void, Long> idMeta = (PropertyMeta<Void, Long>) meta.getIdMeta();
		assertThat(idMeta).isNotNull();
		PropertyMeta<?, ?> counterMeta = meta.getPropertyMetas().get("counter");
		assertThat(counterMeta).isNotNull();

		CounterProperties counterProperties = counterMeta.getCounterProperties();

		assertThat(counterProperties).isNotNull();
		assertThat(counterProperties.getFqcn()).isEqualTo(
				BeanWithSimpleCounter.class.getCanonicalName());
		assertThat((PropertyMeta<Void, Long>) counterProperties.getIdMeta()).isSameAs(idMeta);
	}

	@Test
	public void should_parse_bean_with_widemap_counter_field() throws Exception
	{
		initEntityParsingContext(BeanWithWideMapCounter.class);
		EntityMeta meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		assertThat(entityContext.getHasSimpleCounter()).isFalse();
		PropertyMeta<Void, Long> idMeta = (PropertyMeta<Void, Long>) meta.getIdMeta();
		assertThat(idMeta).isNotNull();
		PropertyMeta<?, ?> counterMeta = meta.getPropertyMetas().get("counters");
		assertThat(counterMeta).isNotNull();

		CounterProperties counterProperties = counterMeta.getCounterProperties();

		assertThat(counterProperties).isNotNull();
		assertThat(counterProperties.getFqcn()).isEqualTo(
				BeanWithWideMapCounter.class.getCanonicalName());
		assertThat((PropertyMeta<Void, Long>) counterProperties.getIdMeta()).isSameAs(idMeta);
	}

	@Test
	public void should_parse_bean_with_wide_map() throws Exception
	{
		initEntityParsingContext(BeanWithExternalWideMap.class);
		EntityMeta meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		PropertyMeta<?, ?> usersPropertyMeta = meta.getPropertyMetas().get("users");
		assertThat(usersPropertyMeta.type()).isEqualTo(WIDE_MAP);
		assertThat(usersPropertyMeta.getExternalCFName()).isEqualTo("external_users");
		assertThat((Class<Long>) usersPropertyMeta.getIdClass()).isEqualTo(Long.class);
	}

	@Test
	public void should_parse_bean_with_join_wide_map() throws Exception
	{
		initEntityParsingContext(BeanWithExternalJoinWideMap.class);
		EntityMeta meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		PropertyMeta<?, ?> usersPropertyMeta = meta.getPropertyMetas().get("users");
		assertThat(usersPropertyMeta.type()).isEqualTo(JOIN_WIDE_MAP);
		assertThat(usersPropertyMeta.getExternalCFName()).isEqualTo("external_users");
		assertThat((Class<Long>) usersPropertyMeta.getIdClass()).isEqualTo(Long.class);
		assertThat((Class<UserBean>) joinPropertyMetaToBeFilled.get(usersPropertyMeta)).isEqualTo(
				UserBean.class);
	}

	@Test
	public void should_exception_when_entity_has_no_id() throws Exception
	{
		initEntityParsingContext(BeanWithNoId.class);

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The entity '" + BeanWithNoId.class.getCanonicalName()
				+ "' should have at least one field with javax.persistence.Id annotation");
		parser.parseEntity(entityContext);
	}

	@Test
	public void should_exception_when_id_type_not_serializable() throws Exception
	{
		initEntityParsingContext(BeanWithNotSerializableId.class);
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("Value of 'id' should be Serializable");
		parser.parseEntity(entityContext);
	}

	@Test
	public void should_exception_when_entity_has_no_column() throws Exception
	{
		initEntityParsingContext(BeanWithNoColumn.class);
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("The entity '"
						+ BeanWithNoColumn.class.getCanonicalName()
						+ "' should have at least one field with javax.persistence.Column or javax.persistence.JoinColumn annotations");
		parser.parseEntity(entityContext);
	}

	@Test
	public void should_exception_when_entity_has_duplicated_column_name() throws Exception
	{
		initEntityParsingContext(BeanWithDuplicatedColumnName.class);
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The property 'name' is already used for the entity '"
				+ BeanWithDuplicatedColumnName.class.getCanonicalName() + "'");

		parser.parseEntity(entityContext);
	}

	@Test
	public void should_exception_when_entity_has_duplicated_join_column_name() throws Exception
	{
		initEntityParsingContext(BeanWithDuplicatedJoinColumnName.class);
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The property 'name' is already used for the entity '"
				+ BeanWithDuplicatedJoinColumnName.class.getCanonicalName() + "'");

		parser.parseEntity(entityContext);
	}

	@Test
	public void should_parse_wide_row() throws Exception
	{
		initEntityParsingContext(WideRowBean.class);
		EntityMeta meta = parser.parseEntity(entityContext);

		assertThat(meta.isWideRow()).isTrue();

		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat((Class<Long>) meta.getIdMeta().getValueClass()).isEqualTo(Long.class);

		assertThat(meta.getPropertyMetas()).hasSize(1);
		assertThat(meta.getPropertyMetas().get("values").type()).isEqualTo(WIDE_MAP);
	}

	@Test
	public void should_parse_wide_row_with_join() throws Exception
	{
		initEntityParsingContext(WideRowBeanWithJoinEntity.class);
		EntityMeta meta = parser.parseEntity(entityContext);

		assertThat(meta.isWideRow()).isTrue();
		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getIdMeta().getValueClass()).isEqualTo((Class) Long.class);

		Map<String, PropertyMeta<?, ?>> propertyMetas = meta.getPropertyMetas();
		assertThat(propertyMetas).hasSize(1);
		PropertyMeta<?, ?> friendMeta = propertyMetas.get("friends");

		assertThat(friendMeta.type()).isEqualTo(JOIN_WIDE_MAP);

		JoinProperties joinProperties = friendMeta.getJoinProperties();
		assertThat(joinProperties).isNotNull();
		assertThat(joinProperties.getCascadeTypes()).containsExactly(CascadeType.ALL);

		EntityMeta joinEntityMeta = joinProperties.getEntityMeta();
		assertThat(joinEntityMeta).isNull();
	}

	@Test
	public void should_exception_when_wide_row_more_than_one_mapped_column() throws Exception
	{
		initEntityParsingContext(WideRowBeanWithTwoColumns.class);

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The ColumnFamily entity '"
				+ WideRowBeanWithTwoColumns.class.getCanonicalName()
				+ "' should not have more than one property annotated with @Column");

		parser.parseEntity(entityContext);

	}

	@Test
	public void should_exception_when_wide_row_has_wrong_column_type() throws Exception
	{
		initEntityParsingContext(WideRowBeanWithWrongColumnType.class);
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The ColumnFamily entity '"
				+ WideRowBeanWithWrongColumnType.class.getCanonicalName()
				+ "' should have one and only one @Column/@JoinColumn of type WideMap");

		parser.parseEntity(entityContext);

	}

	@Test
	public void should_fill_join_entity_meta_map_with_entity_meta() throws Exception
	{
		initEntityParsingContext(null);

		EntityMeta joinEntityMeta = new EntityMeta();
		joinEntityMeta.setWideRow(false);
		joinEntityMeta.setIdClass(Long.class);

		PropertyMeta<Integer, String> joinPropertyMeta = new PropertyMeta<Integer, String>();
		joinPropertyMeta.setJoinProperties(new JoinProperties());
		joinPropertyMeta.setType(JOIN_WIDE_MAP);
		joinPropertyMeta.setIdClass(Long.class);

		joinPropertyMetaToBeFilled.put(joinPropertyMeta, BeanWithJoinColumnAsWideMap.class);
		entityMetaMap = new HashMap<Class<?>, EntityMeta>();
		entityMetaMap.put(BeanWithJoinColumnAsWideMap.class, joinEntityMeta);
		parser.fillJoinEntityMeta(entityContext, entityMetaMap);

		assertThat(joinPropertyMeta.getJoinProperties().getEntityMeta()).isSameAs(joinEntityMeta);
	}

	@Test
	public void should_exception_when_join_entity_is_a_wide_row() throws Exception
	{
		initEntityParsingContext(BeanWithJoinColumnAsWideMap.class);

		EntityMeta joinEntityMeta = new EntityMeta();
		joinEntityMeta.setWideRow(true);
		joinEntityMeta.setClassName(BeanWithJoinColumnAsWideMap.class.getCanonicalName());
		PropertyMeta<Integer, String> joinPropertyMeta = new PropertyMeta<Integer, String>();

		joinPropertyMetaToBeFilled.put(joinPropertyMeta, BeanWithJoinColumnAsWideMap.class);
		entityMetaMap = new HashMap<Class<?>, EntityMeta>();
		entityMetaMap.put(BeanWithJoinColumnAsWideMap.class, joinEntityMeta);

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The entity '"
				+ BeanWithJoinColumnAsWideMap.class.getCanonicalName()
				+ "' is a Wide row and cannot be a join entity");

		parser.fillJoinEntityMeta(entityContext, entityMetaMap);

	}

	@Test
	public void should_exception_when_no_entity_meta_found_for_join_property() throws Exception
	{
		initEntityParsingContext(null);

		PropertyMeta<Integer, String> joinPropertyMeta = new PropertyMeta<Integer, String>();

		joinPropertyMetaToBeFilled.put(joinPropertyMeta, BeanWithJoinColumnAsWideMap.class);
		entityMetaMap = new HashMap<Class<?>, EntityMeta>();

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("Cannot find mapping for join entity '"
				+ BeanWithJoinColumnAsWideMap.class.getCanonicalName() + "'");

		parser.fillJoinEntityMeta(entityContext, entityMetaMap);

	}

	private <T> void initEntityParsingContext(Class<T> entityClass)
	{
		entityContext = new EntityParsingContext( //
				joinPropertyMetaToBeFilled, //
				configContext, entityClass);
	}
}
