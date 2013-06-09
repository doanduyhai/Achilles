package info.archinnov.achilles.query;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import mapping.entity.TweetMultiKey;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * SliceQueryValidatorTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceQueryValidatorTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private SliceQueryValidator validator = new SliceQueryValidator();

	private PropertyMeta<Void, TweetMultiKey> pm;

	@Before
	public void setUp() throws Exception
	{
		pm = new PropertyMeta<Void, TweetMultiKey>();
		MultiKeyProperties multiKeyProperties = new MultiKeyProperties();
		multiKeyProperties.setComponentNames(Arrays.asList("id", "author", "retweetCount"));

		Method idGetter = TweetMultiKey.class.getDeclaredMethod("getId");
		Method authorGetter = TweetMultiKey.class.getDeclaredMethod("getAuthor");
		Method retweetCountGetter = TweetMultiKey.class.getDeclaredMethod("getRetweetCount");

		multiKeyProperties.setComponentGetters(Arrays.asList(idGetter, authorGetter,
				retweetCountGetter));

		pm.setMultiKeyProperties(multiKeyProperties);
		pm.setType(PropertyType.COMPOUND_KEY);
	}

	@Test
	public void should_find_last_non_null_index() throws Exception
	{
		List<Object> components = Arrays.<Object> asList("a", "b", "c");
		int actual = validator.findLastNonNullIndexForComponents("property", components);
		assertThat(actual).isEqualTo(2);

		components = Arrays.<Object> asList("a", "b", null, null);
		actual = validator.findLastNonNullIndexForComponents("property", components);
		assertThat(actual).isEqualTo(1);

		components = Arrays.<Object> asList();
		actual = validator.findLastNonNullIndexForComponents("property", components);
		assertThat(actual).isEqualTo(-1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_null_component_in_middle_of_compound_key() throws Exception
	{
		List<Object> components = Arrays.<Object> asList("a", null, "c");
		validator.findLastNonNullIndexForComponents("property", components);
	}

	@Test
	public void should_validate_single_key() throws Exception
	{
		pm.setSingleKey(true);
		validator.validateCompoundKeys(pm, 10L, 11L);
	}

	@Test
	public void should_exception_when_single_key_not_in_correct_order() throws Exception
	{
		pm.setSingleKey(true);
		exception.expect(AchillesException.class);
		exception
				.expectMessage("For slice query, start value should be lesser or equal to end value");
		validator.validateCompoundKeys(pm, 11L, 10L);
	}

	@Test
	public void should_validate_bounds() throws Exception
	{
		UUID uuid1 = new UUID(10, 11);

		TweetMultiKey key1 = new TweetMultiKey(uuid1, "author", 3);
		TweetMultiKey key2 = new TweetMultiKey(uuid1, "author", 4);

		validator.validateCompoundKeys(pm, key1, key2);

		key1 = new TweetMultiKey(uuid1, "author", 2);
		key2 = new TweetMultiKey(uuid1, "author", 3);

		validator.validateCompoundKeys(pm, key1, key2);

		key1 = new TweetMultiKey(uuid1, "author", null);
		key2 = new TweetMultiKey(uuid1, "author", 3);

		validator.validateCompoundKeys(pm, key1, key2);

		key1 = new TweetMultiKey(uuid1, "a", null);
		key2 = new TweetMultiKey(uuid1, "b", null);

		validator.validateCompoundKeys(pm, key1, key2);

		key1 = new TweetMultiKey(uuid1, "author", null);
		key2 = new TweetMultiKey(uuid1, "author", 5);

		validator.validateCompoundKeys(pm, key1, key2);

		key1 = new TweetMultiKey(uuid1, "a", null);
		key2 = new TweetMultiKey(uuid1, "b", null);

		validator.validateCompoundKeys(pm, key1, key2);

		key1 = new TweetMultiKey(uuid1, null, null);
		key2 = new TweetMultiKey(uuid1, "b", null);

		validator.validateCompoundKeys(pm, key1, key2);

		key1 = new TweetMultiKey(uuid1, "a", null);
		key2 = new TweetMultiKey(uuid1, null, null);

		validator.validateCompoundKeys(pm, key1, key2);

		key1 = new TweetMultiKey(uuid1, null, null);
		key2 = new TweetMultiKey(uuid1, null, null);

		validator.validateCompoundKeys(pm, key1, key2);

		validator.validateCompoundKeys(pm, key1, null);
		validator.validateCompoundKeys(pm, null, key2);
	}

	@Test
	public void should_exception_when_partition_key_null() throws Exception
	{
		UUID uuid2 = new UUID(10, 12);

		TweetMultiKey key1 = new TweetMultiKey(null, null, null);
		TweetMultiKey key2 = new TweetMultiKey(uuid2, null, null);

		exception.expect(AchillesException.class);
		exception.expectMessage("Partition key should not be null for start compound key [,,]");
		validator.validateCompoundKeys(pm, key1, key2);
	}

	@Test
	public void should_exception_when_partition_key_not_equal() throws Exception
	{
		UUID uuid1 = new UUID(10, 11);
		UUID uuid2 = new UUID(10, 12);

		TweetMultiKey key1 = new TweetMultiKey(uuid1, null, null);
		TweetMultiKey key2 = new TweetMultiKey(uuid2, null, null);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("Partition key should be equals for start and end compound keys : [["
						+ uuid1 + ",,],[" + uuid2 + ",,]]");
		validator.validateCompoundKeys(pm, key1, key2);
	}

	@Test
	public void should_exception_when_invalid_compound_keys() throws Exception
	{
		UUID uuid1 = new UUID(10, 11);

		TweetMultiKey key1 = new TweetMultiKey(uuid1, null, null);
		TweetMultiKey key2 = new TweetMultiKey(uuid1, "a", 1);

		exception.expect(AchillesException.class);
		exception.expectMessage("Start compound key [" + uuid1 + ",,] and end compound key ["
				+ uuid1 + ",a,1] are not valid for slice query");
		validator.validateCompoundKeys(pm, key1, key2);
	}

	@Test
	public void should_exception_when_components_are_not_equal_case1() throws Exception
	{
		UUID uuid1 = new UUID(10, 11);
		TweetMultiKey key1 = new TweetMultiKey(uuid1, "a", 1);
		TweetMultiKey key2 = new TweetMultiKey(uuid1, "b", 2);

		exception.expect(AchillesException.class);
		exception.expectMessage("[1]th component of compound keys [[" + uuid1 + ",a,1],[" + uuid1
				+ ",b,2] should be equal");
		validator.validateCompoundKeys(pm, key1, key2);
	}

	@Test
	public void should_exception_when_components_are_not_equal_case2() throws Exception
	{
		UUID uuid1 = new UUID(10, 11);
		TweetMultiKey key1 = new TweetMultiKey(uuid1, "a", null);
		TweetMultiKey key2 = new TweetMultiKey(uuid1, "b", 2);

		exception.expect(AchillesException.class);
		exception.expectMessage("[1]th component of compound keys [[" + uuid1 + ",a,],[" + uuid1
				+ ",b,2] should be equal");
		validator.validateCompoundKeys(pm, key1, key2);
	}

	@Test
	public void should_exception_when_last_components_are_equal() throws Exception
	{
		UUID uuid1 = new UUID(10, 11);
		TweetMultiKey key1 = new TweetMultiKey(uuid1, "a", 1);
		TweetMultiKey key2 = new TweetMultiKey(uuid1, "a", 1);

		exception.expect(AchillesException.class);
		exception.expectMessage("For slice query, last component of start compound key [" + uuid1
				+ ",a,1] should be strictly lesser to last component of end compound key [" + uuid1
				+ ",a,1]");
		validator.validateCompoundKeys(pm, key1, key2);
	}

	@Test
	public void should_exception_when_both_compound_keys_null() throws Exception
	{
		exception.expect(AchillesException.class);
		exception
				.expectMessage("Start and end compound keys for Slice Query should not be both null");
		validator.validateCompoundKeys(pm, null, null);
	}
}
