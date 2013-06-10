package info.archinnov.achilles.query;

import static org.fest.assertions.api.Assertions.*;
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
@SuppressWarnings("rawtypes")
public class SliceQueryValidatorTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private SliceQueryValidator validator = new SliceQueryValidator();

    private PropertyMeta<Void, TweetMultiKey> pm;

    @Before
    public void setUp() throws Exception {
        pm = new PropertyMeta<Void, TweetMultiKey>();
        MultiKeyProperties multiKeyProperties = new MultiKeyProperties();
        multiKeyProperties.setComponentNames(Arrays.asList("id", "author", "retweetCount"));

        Method idGetter = TweetMultiKey.class.getDeclaredMethod("getId");
        Method authorGetter = TweetMultiKey.class.getDeclaredMethod("getAuthor");
        Method retweetCountGetter = TweetMultiKey.class.getDeclaredMethod("getRetweetCount");

        multiKeyProperties.setComponentGetters(Arrays.asList(idGetter, authorGetter, retweetCountGetter));

        pm.setMultiKeyProperties(multiKeyProperties);
        pm.setType(PropertyType.COMPOUND_KEY);
    }

    @Test
    public void should_find_last_non_null_index() throws Exception {
        List<Comparable> components = Arrays.<Comparable> asList("a", "b", "c");
        int actual = validator.findLastNonNullIndexForComponents(components);
        assertThat(actual).isEqualTo(2);

        components = Arrays.<Comparable> asList("a", "b", null, null);
        actual = validator.findLastNonNullIndexForComponents(components);
        assertThat(actual).isEqualTo(1);

        components = Arrays.<Comparable> asList();
        actual = validator.findLastNonNullIndexForComponents(components);
        assertThat(actual).isEqualTo(-1);

        actual = validator.findLastNonNullIndexForComponents(null);
        assertThat(actual).isEqualTo(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_exception_when_null_component_in_middle_of_clustering_keys() throws Exception {
        List<Comparable> components = Arrays.<Comparable> asList("a", null, "c");
        validator.findLastNonNullIndexForComponents(components);
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_exception_when_null_component_at_beginning_of_clustering_keys() throws Exception {
        List<Comparable> components = Arrays.<Comparable> asList(null, null, "c");
        validator.findLastNonNullIndexForComponents(components);
    }

    @Test
    public void should_validate_single_key() throws Exception {
        pm.setSingleKey(true);
        validator.validateClusteringKeys(pm, Arrays.<Comparable> asList(10L), Arrays.<Comparable> asList(11L));
    }

    @Test
    public void should_exception_when_single_key_not_in_correct_order() throws Exception {
        pm.setSingleKey(true);
        exception.expect(AchillesException.class);
        exception
                .expectMessage("Start clustering last component should be strictly 'less' than end clustering last component: [[11],[10]");
        validator.validateClusteringKeys(pm, Arrays.<Comparable> asList(11L), Arrays.<Comparable> asList(10L));
    }

    @Test
    public void should_validate_clustering_keys() throws Exception {
        UUID uuid1 = new UUID(10, 11);
        UUID uuid2 = new UUID(10, 12);

        List<Comparable> start = Arrays.<Comparable> asList(uuid1, "author", 3);
        List<Comparable> end = Arrays.<Comparable> asList(uuid1, "author", 4);
        validator.validateClusteringKeys(pm, start, end);

        start = Arrays.<Comparable> asList(uuid1, "author", 3);
        end = Arrays.<Comparable> asList(uuid1, "author", null);
        validator.validateClusteringKeys(pm, start, end);

        start = Arrays.<Comparable> asList(uuid1, "author", null);
        end = Arrays.<Comparable> asList(uuid1, "author", 3);
        validator.validateClusteringKeys(pm, start, end);

        start = Arrays.<Comparable> asList(uuid1, "a", null);
        end = Arrays.<Comparable> asList(uuid1, "b", null);
        validator.validateClusteringKeys(pm, start, end);

        start = Arrays.<Comparable> asList(uuid1, "a", null);
        end = Arrays.<Comparable> asList(uuid1, null, null);
        validator.validateClusteringKeys(pm, start, end);

        start = Arrays.<Comparable> asList(uuid1, null, null);
        end = Arrays.<Comparable> asList(uuid1, "b", null);
        validator.validateClusteringKeys(pm, start, end);

        start = Arrays.<Comparable> asList(uuid1, null, null);
        end = Arrays.<Comparable> asList(uuid2, null, null);
        validator.validateClusteringKeys(pm, start, end);

        start = Arrays.<Comparable> asList(null, null, null);
        end = Arrays.<Comparable> asList(uuid2, null, null);
        validator.validateClusteringKeys(pm, start, end);

        start = Arrays.<Comparable> asList(uuid1, null, null);
        end = Arrays.<Comparable> asList(null, null, null);
        validator.validateClusteringKeys(pm, start, end);

        validator.validateClusteringKeys(pm, start, null);
        validator.validateClusteringKeys(pm, null, end);
    }

    @Test
    public void should_exception_when_components_strictly_equal() throws Exception {
        UUID uuid1 = new UUID(10, 12);

        List<Comparable> start = Arrays.<Comparable> asList(uuid1, "a", null);
        List<Comparable> end = Arrays.<Comparable> asList(uuid1, "a", null);

        exception.expect(AchillesException.class);
        exception
                .expectMessage("Start clustering last component should be strictly 'less' than end clustering last component: [["
                        + uuid1 + ",a,],[" + uuid1 + ",a,]");
        validator.validateClusteringKeys(pm, start, end);
    }

    @Test
    public void should_exception_when_all_components_strictly_equal() throws Exception {
        UUID uuid1 = new UUID(10, 12);

        List<Comparable> start = Arrays.<Comparable> asList(uuid1, "a", 10);
        List<Comparable> end = Arrays.<Comparable> asList(uuid1, "a", 10);

        exception.expect(AchillesException.class);
        exception
                .expectMessage("Start clustering last component should be strictly 'less' than end clustering last component: [["
                        + uuid1 + ",a,10],[" + uuid1 + ",a,10]");
        validator.validateClusteringKeys(pm, start, end);
    }

    @Test
    public void should_exception_when_too_many_end_components() throws Exception {
        UUID uuid1 = new UUID(10, 11);

        List<Comparable> start = Arrays.<Comparable> asList(uuid1, null, null);
        List<Comparable> end = Arrays.<Comparable> asList(uuid1, "a", 1);

        exception.expect(AchillesException.class);
        exception.expectMessage("There should be no more than 1 component difference between clustering keys: [["
                + uuid1 + ",,],[" + uuid1 + ",a,1]");
        validator.validateClusteringKeys(pm, start, end);
    }

    @Test
    public void should_exception_when_too_many_start_components() throws Exception {
        UUID uuid1 = new UUID(10, 11);

        List<Comparable> start = Arrays.<Comparable> asList(uuid1, "a", 1);
        List<Comparable> end = Arrays.<Comparable> asList(uuid1, null, null);

        exception.expect(AchillesException.class);
        exception.expectMessage("There should be no more than 1 component difference between clustering keys: [["
                + uuid1 + ",a,1],[" + uuid1 + ",,]");
        validator.validateClusteringKeys(pm, start, end);
    }

    @Test
    public void should_exception_when_components_are_not_equal_case1() throws Exception {
        UUID uuid1 = new UUID(10, 11);
        List<Comparable> start = Arrays.<Comparable> asList(uuid1, "a", 1);
        List<Comparable> end = Arrays.<Comparable> asList(uuid1, "b", 2);

        exception.expect(AchillesException.class);
        exception.expectMessage("2th component for clustering keys should be equal: [[" + uuid1 + ",a,1],[" + uuid1
                + ",b,2]");
        validator.validateClusteringKeys(pm, start, end);
    }

    @Test
    public void should_exception_when_components_are_not_equal_case2() throws Exception {
        UUID uuid1 = new UUID(10, 11);
        List<Comparable> start = Arrays.<Comparable> asList(uuid1, "a", null);
        List<Comparable> end = Arrays.<Comparable> asList(uuid1, "b", 2);

        exception.expect(AchillesException.class);
        exception.expectMessage("2th component for clustering keys should be equal: [[" + uuid1 + ",a,],[" + uuid1
                + ",b,2]");
        validator.validateClusteringKeys(pm, start, end);
    }

    @Test
    public void should_exception_when_last_components_are_equal() throws Exception {
        UUID uuid1 = new UUID(10, 11);
        List<Comparable> start = Arrays.<Comparable> asList(uuid1, "a", 1L);
        List<Comparable> end = Arrays.<Comparable> asList(uuid1, "a", 1L);

        exception.expect(AchillesException.class);
        exception
                .expectMessage("Start clustering last component should be strictly 'less' than end clustering last component: [["
                        + uuid1 + ",a,1],[" + uuid1 + ",a,1]");
        validator.validateClusteringKeys(pm, start, end);
    }

    @Test
    public void should_exception_when_last_components_not_same_type() throws Exception {
        UUID uuid1 = new UUID(10, 11);
        List<Comparable> start = Arrays.<Comparable> asList(uuid1, "a", 1);
        List<Comparable> end = Arrays.<Comparable> asList(uuid1, "a", 1L);

        exception.expect(AchillesException.class);
        exception.expectMessage("3th component for clustering keys should be of same type: [[" + uuid1 + ",a,1],["
                + uuid1 + ",a,1]");
        validator.validateClusteringKeys(pm, start, end);
    }

    @Test
    public void should_exception_when_last_commont_components_not_same_type() throws Exception {
        UUID uuid1 = new UUID(10, 11);
        List<Comparable> start = Arrays.<Comparable> asList(uuid1, 10L, null);
        List<Comparable> end = Arrays.<Comparable> asList(uuid1, "a", 1L);

        exception.expect(AchillesException.class);
        exception.expectMessage("2th component for clustering keys should be of same type: [[" + uuid1 + ",10,],["
                + uuid1 + ",a,1]");
        validator.validateClusteringKeys(pm, start, end);
    }
}
