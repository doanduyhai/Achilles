package info.archinnov.achilles.helper;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import mapping.entity.CompleteBean;
import mapping.entity.TweetMultiKey;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.BeanWithColumnFamilyName;
import parser.entity.ChildBean;

/**
 * AchillesEntityIntrospectorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class AchillesEntityIntrospectorTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@Mock
	private PropertyMeta<TweetMultiKey, String> wideMapMeta;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> getterMetas;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> setterMetas;

	private final AchillesEntityIntrospector introspector = new AchillesEntityIntrospector();

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	@Test
	public void should_derive_getter() throws Exception
	{

		class Test
		{

			Boolean old;
		}

		String[] getterNames = introspector.deriveGetterName(Test.class.getDeclaredField("old"));
		assertThat(getterNames).hasSize(1);
		assertThat(getterNames[0]).isEqualTo("getOld");
	}

	@Test
	public void should_derive_getter_for_boolean_primitive() throws Exception
	{

		class Test
		{

			boolean old;
		}

		String[] getterNames = introspector.deriveGetterName(Test.class.getDeclaredField("old"));
		assertThat(getterNames).hasSize(2);
		assertThat(getterNames[0]).isEqualTo("isOld");
		assertThat(getterNames[1]).isEqualTo("getOld");
	}

	@Test
	public void should_derive_setter() throws Exception
	{
		class Test
		{
			boolean a;
		}

		assertThat(introspector.deriveSetterName(Test.class.getDeclaredField("a"))).isEqualTo(
				"setA");
	}

	@Test
	public void should_exception_when_no_getter() throws Exception
	{

		class Test
		{
			String name;
		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The getter for field 'name' of type 'null' does not exist");

		introspector.findGetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_no_setter() throws Exception
	{

		class Test
		{
			String name;

			public String getA()
			{
				return name;
			}
		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The setter for field 'name' of type 'null' does not exist");

		introspector.findSetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_incorrect_getter() throws Exception
	{

		class Test
		{
			String name;

			public Long getName()
			{
				return 1L;
			}

		}
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("The getter for field 'name' of type 'null' does not return correct type");

		introspector.findGetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_setter_returning_wrong_type() throws Exception
	{

		class Test
		{
			String name;

			public String getName()
			{
				return name;
			}

			public Long setName(String name)
			{
				return 1L;
			}

		}
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("The setter for field 'name' of type 'null' does not return correct type or does not have the correct parameter");

		introspector.findSetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_setter_taking_wrong_type() throws Exception
	{

		class Test
		{
			String name;

			public String getName()
			{
				return name;
			}

			public void setName(Long name)
			{}

		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("The setter for field 'name' of type 'null' does not exist or is incorrect");

		introspector.findSetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_find_getter_from_boolean_as_isOld() throws Exception
	{
		class Test
		{
			boolean old;

			public boolean isOld()
			{
				return old;
			}

			public void setOld(boolean old)
			{
				this.old = old;
			}
		}

		Method[] accessors = introspector.findAccessors(Test.class,
				Test.class.getDeclaredField("old"));

		assertThat(accessors[0].getName()).isEqualTo("isOld");
	}

	@Test
	public void should_find_getter_from_boolean_as_getOld() throws Exception
	{
		class Test
		{
			boolean old;

			public boolean getOld()
			{
				return old;
			}

			public void setOld(boolean old)
			{
				this.old = old;
			}
		}

		Method[] accessors = introspector.findAccessors(Test.class,
				Test.class.getDeclaredField("old"));

		assertThat(accessors[0].getName()).isEqualTo("getOld");
	}

	@Test
	public void should_find_accessors() throws Exception
	{

		Method[] accessors = introspector.findAccessors(Bean.class,
				Bean.class.getDeclaredField("complicatedAttributeName"));

		assertThat(accessors).hasSize(2);
		assertThat(accessors[0].getName()).isEqualTo("getComplicatedAttributeName");
		assertThat(accessors[1].getName()).isEqualTo("setComplicatedAttributeName");
	}

	@Test
	public void should_find_accessors_from_collection_types() throws Exception
	{

		Method[] accessors = introspector.findAccessors(ComplexBean.class,
				ComplexBean.class.getDeclaredField("friends"));

		assertThat(accessors).hasSize(2);
		assertThat(accessors[0].getName()).isEqualTo("getFriends");
		assertThat(accessors[1].getName()).isEqualTo("setFriends");
	}

	@Test
	public void should_find_accessors_from_widemap_type() throws Exception
	{
		Method[] accessors = introspector.findAccessors(CompleteBean.class,
				CompleteBean.class.getDeclaredField("tweets"));

		assertThat(accessors).hasSize(2);
		assertThat(accessors[0].getName()).isEqualTo("getTweets");
		assertThat(accessors[1]).isNull();
	}

	@Test
	public void should_find_accessors_from_counter_type() throws Exception
	{
		Method[] accessors = introspector.findAccessors(CompleteBean.class,
				CompleteBean.class.getDeclaredField("count"));

		assertThat(accessors).hasSize(2);
		assertThat(accessors[0].getName()).isEqualTo("getCount");
		assertThat(accessors[1]).isNull();
	}

	@Test
	public void should_get_inherited_fields() throws Exception
	{
		List<Field> fields = introspector.getInheritedPrivateFields(ChildBean.class);

		assertThat(fields).hasSize(4);
		assertThat(fields.get(0).getName()).isEqualTo("nickname");
		assertThat(fields.get(1).getName()).isEqualTo("name");
		assertThat(fields.get(2).getName()).isEqualTo("address");
		assertThat(fields.get(3).getName()).isEqualTo("id");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_get_inherited_field_by_annotation() throws Exception
	{
		Field id = introspector.getInheritedPrivateFields(ChildBean.class, Id.class);

		assertThat(id.getName()).isEqualTo("id");
		assertThat(id.getType()).isEqualTo((Class) Long.class);
	}

	@Test
	public void should_not_get_inherited_field_by_annotation_when_no_match() throws Exception
	{
		assertThat(
				introspector.getInheritedPrivateFields(ChildBean.class,
						javax.persistence.Basic.class)).isNull();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_get_inherited_field_by_annotation_and_name() throws Exception
	{
		Field address = introspector.getInheritedPrivateFields(ChildBean.class, Column.class,
				"address");

		assertThat(address.getName()).isEqualTo("address");
		assertThat(address.getType()).isEqualTo((Class) String.class);
	}

	@Test
	public void should_not_get_inherited_field_by_annotation_and_name_when_no_match()
			throws Exception
	{
		assertThat(
				introspector.getInheritedPrivateFields(ChildBean.class,
						javax.persistence.Basic.class, "address")).isNull();
	}

	@Test
	public void should_find_serial_version_UID() throws Exception
	{
		class Test
		{
			private static final long serialVersionUID = 1542L;
		}

		Long serialUID = introspector.findSerialVersionUID(Test.class);
		assertThat(serialUID).isEqualTo(1542L);
	}

	@Test
	public void should_exception_when_no_serial_version_UID() throws Exception
	{
		class Test
		{
			private static final long fieldName = 1542L;
		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("The 'serialVersionUID' property should be declared for entity 'null'");

		introspector.findSerialVersionUID(Test.class);
	}

	@Test
	public void should_infer_column_family_from_annotation() throws Exception
	{
		String cfName = introspector.inferColumnFamilyName(BeanWithColumnFamilyName.class,
				"canonicalName");
		assertThat(cfName).isEqualTo("myOwnCF");
	}

	@Test
	public void should_infer_column_family_from_default_name() throws Exception
	{
		String cfName = introspector.inferColumnFamilyName(CompleteBean.class, "canonicalName");
		assertThat(cfName).isEqualTo("canonicalName");
	}

	@Test
	public void should_infer_column_family_from_default_name_when_empty_annotation_name()
			throws Exception
	{
		@Table(name = "")
		class Test
		{

		}
		String cfName = introspector.inferColumnFamilyName(Test.class, "canonicalName");
		assertThat(cfName).isEqualTo("canonicalName");
	}

	@Test
	public void should_find_any_any_consistency_level() throws Exception
	{
		@Consistency(read = ANY, write = LOCAL_QUORUM)
		class Test
		{}

		Pair<ConsistencyLevel, ConsistencyLevel> levels = introspector.findConsistencyLevels(
				Test.class, policy);

		assertThat(levels.left).isEqualTo(ANY);
		assertThat(levels.right).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_find_one_one_consistency_level_by_default() throws Exception
	{
		class Test
		{}

		Pair<ConsistencyLevel, ConsistencyLevel> levels = introspector.findConsistencyLevels(
				Test.class, policy);

		assertThat(levels.left).isEqualTo(ONE);
		assertThat(levels.right).isEqualTo(ONE);
	}

	@Test
	public void should_get_defaul_global_read_consistency_from_config() throws Exception
	{
		when(policy.getDefaultGlobalReadConsistencyLevel()).thenReturn(LOCAL_QUORUM);
		assertThat(introspector.getDefaultGlobalReadConsistency(policy)).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_get_defaul_global_read_consistency() throws Exception
	{
		assertThat(introspector.getDefaultGlobalReadConsistency(policy)).isEqualTo(ONE);
	}

	@Test
	public void should_get_defaul_global_write_consistency_from_config() throws Exception
	{
		when(policy.getDefaultGlobalWriteConsistencyLevel()).thenReturn(LOCAL_QUORUM);
		assertThat(introspector.getDefaultGlobalWriteConsistency(policy)).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_get_defaul_global_write_consistency() throws Exception
	{
		assertThat(introspector.getDefaultGlobalWriteConsistency(policy)).isEqualTo(ONE);
	}

	class Bean
	{

		private String complicatedAttributeName;

		public String getComplicatedAttributeName()
		{
			return complicatedAttributeName;
		}

		public void setComplicatedAttributeName(String complicatedAttributeName)
		{
			this.complicatedAttributeName = complicatedAttributeName;
		}
	}

	class ComplexBean
	{
		private List<String> friends;

		public List<String> getFriends()
		{
			return friends;
		}

		public void setFriends(List<String> friends)
		{
			this.friends = friends;
		}
	}
}
