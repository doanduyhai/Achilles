package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_JOIN_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static javax.persistence.CascadeType.MERGE;
import static javax.persistence.CascadeType.PERSIST;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.columnFamily.ColumnFamilyCreator;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parser.context.EntityParsingContext;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.serializer.SerializerUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.persistence.CascadeType;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;

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
import parser.entity.ColumnFamilyBean;
import parser.entity.ColumnFamilyBeanWithJoinEntity;
import parser.entity.ColumnFamilyBeanWithTwoColumns;
import parser.entity.ColumnFamilyBeanWithWrongColumnType;
import parser.entity.UserBean;

/**
 * EntityParserTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityParserTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private EntityParser parser;

	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
	private Map<String, GenericEntityDao<?>> entityDaosMap = new HashMap<String, GenericEntityDao<?>>();
	private Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap = new HashMap<String, GenericColumnFamilyDao<?, ?>>();
	private Map<String, HConsistencyLevel> readConsistencyMap = new HashMap<String, HConsistencyLevel>();
	private Map<String, HConsistencyLevel> writeConsistencyMap = new HashMap<String, HConsistencyLevel>();
	private AchillesConfigurableConsistencyLevelPolicy configurableCLPolicy = new AchillesConfigurableConsistencyLevelPolicy(
			ONE, ALL, readConsistencyMap, writeConsistencyMap);

	@Mock
	private ColumnFamilyCreator columnFamilyCreator;

	@Mock
	private Cluster cluster;

	@Mock
	private Keyspace keyspace;

	@Mock
	private CounterDao counterDao;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

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
		entityDaosMap.clear();
		columnFamilyDaosMap.clear();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_parse_entity() throws Exception
	{

		initEntityParsingContext(Bean.class);
		EntityMeta<?> meta = parser.parseEntity(entityContext);

		assertThat(meta.getClassName()).isEqualTo("parser.entity.Bean");
		assertThat(meta.getColumnFamilyName()).isEqualTo("Bean");
		assertThat(meta.getSerialVersionUID()).isEqualTo(1L);
		assertThat((Class<Long>) meta.getIdMeta().getValueClass()).isEqualTo(Long.class);
		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getIdMeta().getValueSerializer().getComparatorType()).isEqualTo(
				LONG_SRZ.getComparatorType());
		assertThat((Serializer<Long>) meta.getIdSerializer()).isEqualTo(LONG_SRZ);
		assertThat(meta.getPropertyMetas()).hasSize(7);

		PropertyMeta<?, ?> name = meta.getPropertyMetas().get("name");
		PropertyMeta<?, ?> age = meta.getPropertyMetas().get("age_in_year");
		PropertyMeta<Void, String> friends = (PropertyMeta<Void, String>) meta.getPropertyMetas()
				.get("friends");
		PropertyMeta<Void, String> followers = (PropertyMeta<Void, String>) meta.getPropertyMetas()
				.get("followers");
		PropertyMeta<Integer, String> preferences = (PropertyMeta<Integer, String>) meta
				.getPropertyMetas().get("preferences");

		PropertyMeta<Void, UserBean> creator = (PropertyMeta<Void, UserBean>) meta
				.getPropertyMetas().get("creator");
		PropertyMeta<String, UserBean> linkedUsers = (PropertyMeta<String, UserBean>) meta
				.getPropertyMetas().get("linked_users");

		assertThat(name).isNotNull();
		assertThat(age).isNotNull();
		assertThat(friends).isNotNull();
		assertThat(followers).isNotNull();
		assertThat(preferences).isNotNull();
		assertThat(creator).isNotNull();
		assertThat(linkedUsers).isNotNull();

		assertThat(name.getPropertyName()).isEqualTo("name");
		assertThat((Class<String>) name.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) name.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(name.type()).isEqualTo(SIMPLE);
		assertThat(name.getReadConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
		assertThat(name.getWriteConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);

		assertThat(age.getPropertyName()).isEqualTo("age_in_year");
		assertThat((Class<Long>) age.getValueClass()).isEqualTo(Long.class);
		assertThat((Serializer<Long>) age.getValueSerializer()).isEqualTo(LONG_SRZ);
		assertThat(age.type()).isEqualTo(SIMPLE);
		assertThat(age.getReadConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
		assertThat(age.getWriteConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);

		assertThat(friends.getPropertyName()).isEqualTo("friends");
		assertThat(friends.getValueClass()).isEqualTo(String.class);
		assertThat(friends.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(friends.type()).isEqualTo(PropertyType.LAZY_LIST);
		assertThat(friends.newListInstance()).isNotNull();
		assertThat(friends.newListInstance()).isEmpty();
		assertThat(friends.type().isLazy()).isTrue();
		assertThat((Class<ArrayList>) friends.newListInstance().getClass()).isEqualTo(
				ArrayList.class);
		assertThat(friends.getReadConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
		assertThat(friends.getWriteConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);

		assertThat(followers.getPropertyName()).isEqualTo("followers");
		assertThat(followers.getValueClass()).isEqualTo(String.class);
		assertThat(followers.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(followers.type()).isEqualTo(PropertyType.SET);
		assertThat(followers.newSetInstance()).isNotNull();
		assertThat(followers.newSetInstance()).isEmpty();
		assertThat((Class<HashSet>) followers.newSetInstance().getClass()).isEqualTo(HashSet.class);
		assertThat(followers.getReadConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
		assertThat(followers.getWriteConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);

		assertThat(preferences.getPropertyName()).isEqualTo("preferences");
		assertThat(preferences.getValueClass()).isEqualTo(String.class);
		assertThat(preferences.getValueSerializer()).isEqualTo(STRING_SRZ);
		assertThat(preferences.type()).isEqualTo(PropertyType.MAP);
		assertThat(preferences.getKeyClass()).isEqualTo(Integer.class);
		assertThat(preferences.getKeySerializer()).isEqualTo(SerializerUtils.INT_SRZ);
		assertThat(preferences.newMapInstance()).isNotNull();
		assertThat(preferences.newMapInstance()).isEmpty();
		assertThat((Class<HashMap>) preferences.newMapInstance().getClass()).isEqualTo(
				HashMap.class);
		assertThat(preferences.getReadConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
		assertThat(preferences.getWriteConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);

		assertThat(creator.getPropertyName()).isEqualTo("creator");
		assertThat(creator.getValueClass()).isEqualTo(UserBean.class);
		assertThat((Serializer) creator.getValueSerializer()).isEqualTo(SerializerUtils.OBJECT_SRZ);
		assertThat(creator.type()).isEqualTo(JOIN_SIMPLE);
		assertThat(creator.getJoinProperties().getCascadeTypes()).containsExactly(CascadeType.ALL);

		assertThat(linkedUsers.getPropertyName()).isEqualTo("linked_users");
		assertThat(linkedUsers.getValueClass()).isEqualTo(UserBean.class);
		assertThat((Serializer) linkedUsers.getValueSerializer()).isEqualTo(
				SerializerUtils.OBJECT_SRZ);
		assertThat(linkedUsers.type()).isEqualTo(JOIN_WIDE_MAP);
		assertThat(linkedUsers.getJoinProperties().getCascadeTypes()).contains(PERSIST, MERGE);

		assertThat((Class) joinPropertyMetaToBeFilled.get(creator)).isEqualTo(UserBean.class);
		assertThat((Class) joinPropertyMetaToBeFilled.get(linkedUsers)).isEqualTo(UserBean.class);

		assertThat(meta.getConsistencyLevels().left).isEqualTo(ConsistencyLevel.ONE);
		assertThat(meta.getConsistencyLevels().right).isEqualTo(ConsistencyLevel.ALL);

		assertThat(configurableCLPolicy.getConsistencyLevelForRead(meta.getColumnFamilyName()))
				.isEqualTo(HConsistencyLevel.ONE);
		assertThat(configurableCLPolicy.getConsistencyLevelForWrite(meta.getColumnFamilyName()))
				.isEqualTo(HConsistencyLevel.ALL);
	}

	@Test
	public void should_parse_entity_with_table_name() throws Exception
	{

		initEntityParsingContext(BeanWithColumnFamilyName.class);

		EntityMeta<?> meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		assertThat(meta.getColumnFamilyName()).isEqualTo("myOwnCF");
	}

	@Test
	public void should_parse_inherited_bean() throws Exception
	{
		initEntityParsingContext(ChildBean.class);
		EntityMeta<?> meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getPropertyMetas().get("name").getPropertyName()).isEqualTo("name");
		assertThat(meta.getPropertyMetas().get("address").getPropertyName()).isEqualTo("address");
		assertThat(meta.getPropertyMetas().get("nickname").getPropertyName()).isEqualTo("nickname");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_bean_with_simple_counter_field() throws Exception
	{
		initEntityParsingContext(BeanWithSimpleCounter.class);
		EntityMeta<?> meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		assertThat(entityContext.getHasCounter()).isTrue();
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

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_bean_with_widemap_counter_field() throws Exception
	{
		initEntityParsingContext(BeanWithWideMapCounter.class);
		EntityMeta<?> meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		assertThat(entityContext.getHasCounter()).isTrue();
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
	public void should_parse_bean_with_external_wide_map() throws Exception
	{
		initEntityParsingContext(BeanWithExternalWideMap.class);
		EntityMeta<?> meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		PropertyMeta<?, ?> usersPropertyMeta = meta.getPropertyMetas().get("users");
		assertThat(usersPropertyMeta.type()).isEqualTo(EXTERNAL_WIDE_MAP);
		ExternalWideMapProperties<?> externalWideMapProperties = usersPropertyMeta
				.getExternalWideMapProperties();

		assertThat(externalWideMapProperties.getExternalColumnFamilyName()).isEqualTo(
				"external_users");
		assertThat(entityContext.getColumnFamilyDaosMap()).isNotEmpty();
		GenericColumnFamilyDao<?, ?> dao = entityContext.getColumnFamilyDaosMap().values().iterator()
				.next();

		assertThat(dao.getColumnFamily()).isEqualTo("external_users");
		assertThat(Whitebox.getInternalState(dao, "valueSerializer")).isEqualTo(STRING_SRZ);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_bean_with_external_join_wide_map() throws Exception
	{
		initEntityParsingContext(BeanWithExternalJoinWideMap.class);
		EntityMeta<?> meta = parser.parseEntity(entityContext);

		assertThat(meta).isNotNull();
		PropertyMeta<?, ?> usersPropertyMeta = meta.getPropertyMetas().get("users");
		assertThat(usersPropertyMeta.type()).isEqualTo(EXTERNAL_JOIN_WIDE_MAP);
		ExternalWideMapProperties<?> externalWideMapProperties = usersPropertyMeta
				.getExternalWideMapProperties();

		assertThat(externalWideMapProperties.getExternalColumnFamilyName()).isEqualTo(
				"external_users");
		assertThat((Class<UserBean>) joinPropertyMetaToBeFilled.get(usersPropertyMeta)).isEqualTo(
				UserBean.class);
		assertThat(entityContext.getColumnFamilyDaosMap()).isEmpty();
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

	@SuppressWarnings("rawtypes")
	@Test
	public void should_parse_column_family() throws Exception
	{
		initEntityParsingContext(ColumnFamilyBean.class);
		EntityMeta<?> meta = parser.parseEntity(entityContext);

		assertThat(meta.isColumnFamilyDirectMapping()).isTrue();

		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat((Class) meta.getIdMeta().getValueClass()).isEqualTo(Long.class);

		assertThat(meta.getPropertyMetas()).hasSize(1);
		assertThat(meta.getPropertyMetas().get("values").type()).isEqualTo(EXTERNAL_WIDE_MAP);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Test
	public void should_parse_column_family_with_join() throws Exception
	{
		initEntityParsingContext(ColumnFamilyBeanWithJoinEntity.class);
		EntityMeta<?> meta = parser.parseEntity(entityContext);

		assertThat(meta.isColumnFamilyDirectMapping()).isTrue();
		assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
		assertThat(meta.getIdMeta().getValueClass()).isEqualTo((Class) Long.class);

		Map<String, PropertyMeta<?, ?>> propertyMetas = meta.getPropertyMetas();
		assertThat(propertyMetas).hasSize(1);
		PropertyMeta<?, ?> friendMeta = propertyMetas.get("friends");

		assertThat(friendMeta.type()).isEqualTo(EXTERNAL_JOIN_WIDE_MAP);

		JoinProperties joinProperties = friendMeta.getJoinProperties();
		assertThat(joinProperties).isNotNull();
		assertThat(joinProperties.getCascadeTypes()).containsExactly(CascadeType.ALL);

		EntityMeta joinEntityMeta = joinProperties.getEntityMeta();
		assertThat(joinEntityMeta).isNull();
	}

	@Test
	public void should_exception_when_wide_row_more_than_one_mapped_column() throws Exception
	{
		initEntityParsingContext(ColumnFamilyBeanWithTwoColumns.class);

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The ColumnFamily entity '"
				+ ColumnFamilyBeanWithTwoColumns.class.getCanonicalName()
				+ "' should not have more than one property annotated with @Column");

		parser.parseEntity(entityContext);

	}

	@Test
	public void should_exception_when_wide_row_has_wrong_column_type() throws Exception
	{
		initEntityParsingContext(ColumnFamilyBeanWithWrongColumnType.class);
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The ColumnFamily entity '"
				+ ColumnFamilyBeanWithWrongColumnType.class.getCanonicalName()
				+ "' should have one and only one @Column/@JoinColumn of type WideMap");

		parser.parseEntity(entityContext);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_fill_join_entity_meta_map_with_entity_meta() throws Exception
	{
		initEntityParsingContext(null);

		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		joinEntityMeta.setColumnFamilyDirectMapping(false);

		PropertyMeta<Integer, String> joinPropertyMeta = new PropertyMeta<Integer, String>();
		joinPropertyMeta.setJoinProperties(new JoinProperties());
		joinPropertyMeta.setType(JOIN_WIDE_MAP);

		joinPropertyMetaToBeFilled.put(joinPropertyMeta, BeanWithJoinColumnAsWideMap.class);
		entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
		entityMetaMap.put(BeanWithJoinColumnAsWideMap.class, joinEntityMeta);
		parser.fillJoinEntityMeta(entityContext, entityMetaMap);

		assertThat((EntityMeta<Long>) joinPropertyMeta.getJoinProperties().getEntityMeta())
				.isSameAs(joinEntityMeta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_fill_join_entity_meta_map_with_entity_meta_for_external() throws Exception
	{
		initEntityParsingContext(null);

		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		joinEntityMeta.setColumnFamilyDirectMapping(false);
		joinEntityMeta.setIdSerializer(LONG_SRZ);

		PropertyMeta<Integer, String> joinPropertyMeta = new PropertyMeta<Integer, String>();
		joinPropertyMeta.setJoinProperties(new JoinProperties());
		joinPropertyMeta.setType(PropertyType.EXTERNAL_JOIN_WIDE_MAP);

		ExternalWideMapProperties<Long> externalWideMapProperties = new ExternalWideMapProperties<Long>(
				"cfExternal", LONG_SRZ);

		joinPropertyMeta.setExternalWideMapProperties(externalWideMapProperties);

		joinPropertyMetaToBeFilled.put(joinPropertyMeta, BeanWithJoinColumnAsWideMap.class);
		entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
		entityMetaMap.put(BeanWithJoinColumnAsWideMap.class, joinEntityMeta);

		parser.fillJoinEntityMeta(entityContext, entityMetaMap);

		assertThat((EntityMeta<Long>) joinPropertyMeta.getJoinProperties().getEntityMeta())
				.isSameAs(joinEntityMeta);

		GenericColumnFamilyDao<?, ?> externalWideMapDao = columnFamilyDaosMap.get("cfExternal");

		assertThat(externalWideMapDao).isNotNull();
		assertThat(Whitebox.getInternalState(externalWideMapDao, "keyspace")).isSameAs(keyspace);
		assertThat(Whitebox.getInternalState(externalWideMapDao, "keySerializer")).isSameAs(
				LONG_SRZ);
		assertThat(Whitebox.getInternalState(externalWideMapDao, "columnFamily")).isSameAs(
				"cfExternal");
		assertThat(Whitebox.getInternalState(externalWideMapDao, "valueSerializer")).isSameAs(
				LONG_SRZ);
	}

	@Test
	public void should_exception_when_join_entity_is_a_direct_cf_mapping() throws Exception
	{
		initEntityParsingContext(BeanWithJoinColumnAsWideMap.class);

		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		joinEntityMeta.setColumnFamilyDirectMapping(true);
		joinEntityMeta.setClassName(BeanWithJoinColumnAsWideMap.class.getCanonicalName());
		PropertyMeta<Integer, String> joinPropertyMeta = new PropertyMeta<Integer, String>();

		joinPropertyMetaToBeFilled.put(joinPropertyMeta, BeanWithJoinColumnAsWideMap.class);
		entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
		entityMetaMap.put(BeanWithJoinColumnAsWideMap.class, joinEntityMeta);

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The entity '"
				+ BeanWithJoinColumnAsWideMap.class.getCanonicalName()
				+ "' is a direct Column Family mapping and cannot be a join entity");

		parser.fillJoinEntityMeta(entityContext, entityMetaMap);

	}

	@Test
	public void should_exception_when_no_entity_meta_found_for_join_property() throws Exception
	{
		initEntityParsingContext(null);

		PropertyMeta<Integer, String> joinPropertyMeta = new PropertyMeta<Integer, String>();

		joinPropertyMetaToBeFilled.put(joinPropertyMeta, BeanWithJoinColumnAsWideMap.class);
		entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("Cannot find mapping for join entity '"
				+ BeanWithJoinColumnAsWideMap.class.getCanonicalName() + "'");

		parser.fillJoinEntityMeta(entityContext, entityMetaMap);

	}

	private <T> void initEntityParsingContext(Class<T> entityClass)
	{
		entityContext = new EntityParsingContext( //
				joinPropertyMetaToBeFilled, //
				entityDaosMap, //
				columnFamilyDaosMap, //
				configurableCLPolicy, //
				counterDao, //
				cluster, keyspace, //
				objectMapperFactory, entityClass);
	}
}
