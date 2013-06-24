package info.archinnov.achilles.compound;

import static info.archinnov.achilles.type.OrderingMode.*;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftCompoundKeyValidatorTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private ThriftCompoundKeyValidator validator;

    @Mock
    private PropertyMeta<?, ?> pm;

    @Before
    public void setUp()
    {
        when(pm.getEntityClassName()).thenReturn("entityClass");
    }

    @Test
    public void should_validate_partition_keys() throws Exception
    {
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class));

        validator.validatePartitionKey(pm, 11L, 13L);
    }

    @Test
    public void should_exception_when_no_partition_key_provided() throws Exception
    {

        exception.expect(AchillesException.class);
        exception
                .expectMessage("There should be at least one partition key provided for querying on entity 'entityClass'");

        validator.validatePartitionKey(pm, (Object[]) null);
    }

    @Test
    public void should_exception_when_null_partition_key_provided() throws Exception
    {

        exception.expect(AchillesException.class);
        exception
                .expectMessage("There should be at least one partition key provided for querying on entity 'entityClass'");

        validator.validatePartitionKey(pm, new Object[0]);
    }

    @Test
    public void should_exception_when_incorrect_type_of_partition_key_provided() throws Exception
    {
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class));

        exception.expect(AchillesException.class);
        exception
                .expectMessage("The type 'java.lang.String' of partition key 'name' for querying on entity 'entityClass' is not valid. It should be 'java.lang.Long'");

        validator.validatePartitionKey(pm, 11L, "name");
    }

    @Test
    public void should_validate_clustering_keys() throws Exception
    {
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class, Integer.class));

        validator.validateClusteringKeys(pm, "name", 13);
    }

    @Test
    public void should_exception_when_no_clustering_key_provided() throws Exception
    {

        exception.expect(AchillesException.class);
        exception
                .expectMessage("There should be at least one clustering key provided for querying on entity 'entityClass'");

        validator.validateClusteringKeys(pm, (Object[]) null);
    }

    @Test
    public void should_exception_when_wrong_type_provided_for_clustering_keys() throws Exception
    {
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class, Integer.class, UUID.class));

        exception.expect(AchillesException.class);
        exception
                .expectMessage("The type 'java.lang.Long' of clustering key '15' for querying on entity 'entityClass' is not valid. It should be 'java.lang.Integer'");

        validator
                .validateClusteringKeys(pm, "name", 15L, TimeUUIDUtils.getUniqueTimeUUIDinMillis());
    }

    @Test
    public void should_exception_when_too_many_values_for_clustering_keys() throws Exception
    {
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class, Integer.class, UUID.class));

        exception.expect(AchillesException.class);
        exception
                .expectMessage("There should be at most 3 value(s) of clustering component(s) provided for querying on entity 'entityClass'");

        validator.validateClusteringKeys(pm, "name", 15L,
                TimeUUIDUtils.getUniqueTimeUUIDinMillis(), 15);
    }

    @Test
    public void should_exception_when_component_not_comparable_for_clustering_key()
            throws Exception
    {
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class, Integer.class, UserBean.class));

        UserBean userBean = new UserBean();
        exception.expect(AchillesException.class);
        exception
                .expectMessage("The type '"
                        + UserBean.class.getCanonicalName()
                        + "' of clustering key '"
                        + userBean
                        + "' for querying on entity 'entityClass' should implement the Comparable<T> interface");

        validator.validateClusteringKeys(pm, "name", 15, userBean);
    }

    @Test
    public void should_skip_validation_when_null_clustering_value() throws Exception {
        when(pm.getComponentClasses()).thenReturn(
                Arrays.<Class<?>> asList(Long.class, String.class, Integer.class, UserBean.class));

        validator.validateClusteringKeys(pm, null, null, null);

    }

    @Test
    public void should_validate_components_for_query_in_ascending_order_for_query()
            throws Exception
    {
        List<Object> start;
        List<Object> end;

        start = Arrays.<Object> asList("a", 13);
        end = Arrays.<Object> asList("a", 13);
        validator.validateComponentsForQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", 13);
        end = Arrays.<Object> asList("a", 14);
        validator.validateComponentsForQuery(start, end, ASCENDING);

        // ///////////////
        start = Arrays.<Object> asList("a", 13);
        end = Arrays.<Object> asList("b", 13);
        validator.validateComponentsForQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", 13);
        end = Arrays.<Object> asList("b", 14);
        validator.validateComponentsForQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", 14);
        end = Arrays.<Object> asList("b", 13);
        validator.validateComponentsForQuery(start, end, ASCENDING);

        // //////////////
        start = Arrays.<Object> asList("a", null);
        end = Arrays.<Object> asList("a", 14);
        validator.validateComponentsForQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", 14);
        end = Arrays.<Object> asList("a", null);
        validator.validateComponentsForQuery(start, end, ASCENDING);

        // /////////////////
        start = Arrays.<Object> asList("a", null);
        end = Arrays.<Object> asList("b", null);
        validator.validateComponentsForQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", null);
        end = Arrays.<Object> asList("b", 14);
        validator.validateComponentsForQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", 14);
        end = Arrays.<Object> asList("b", null);
        validator.validateComponentsForQuery(start, end, ASCENDING);

    }

    @Test
    public void should_validate_components_for_query_in_descending_order_for_query()
            throws Exception
    {
        List<Object> start;
        List<Object> end;

        start = Arrays.<Object> asList("a", 13);
        end = Arrays.<Object> asList("a", 13);
        validator.validateComponentsForQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("a", 14);
        end = Arrays.<Object> asList("a", 13);
        validator.validateComponentsForQuery(start, end, DESCENDING);

        // ///////////////
        start = Arrays.<Object> asList("b", 13);
        end = Arrays.<Object> asList("a", 13);
        validator.validateComponentsForQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("b", 13);
        end = Arrays.<Object> asList("a", 14);
        validator.validateComponentsForQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("b", 14);
        end = Arrays.<Object> asList("a", 13);
        validator.validateComponentsForQuery(start, end, DESCENDING);

        // //////////////
        start = Arrays.<Object> asList("a", null);
        end = Arrays.<Object> asList("a", 14);
        validator.validateComponentsForQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("a", 14);
        end = Arrays.<Object> asList("a", null);
        validator.validateComponentsForQuery(start, end, DESCENDING);

        // //////////////////
        start = Arrays.<Object> asList("b", null);
        end = Arrays.<Object> asList("a", null);
        validator.validateComponentsForQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("b", null);
        end = Arrays.<Object> asList("a", 14);
        validator.validateComponentsForQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("b", 14);
        end = Arrays.<Object> asList("a", null);
        validator.validateComponentsForQuery(start, end, DESCENDING);
    }

    @Test
    public void should_exception_when_start_greater_than_end_for_ascending_for_query()
            throws Exception
    {
        List<Object> start;
        List<Object> end;

        start = Arrays.<Object> asList("a", 14);
        end = Arrays.<Object> asList("a", 13);

        exception.expect(AchillesException.class);
        exception
                .expectMessage("For slice query with ascending order, start component '14' should be lesser or equal to end component '13'");

        validator.validateComponentsForQuery(start, end, ASCENDING);

    }

    @Test
    public void should_exception_when_start_lesser_than_end_for_descending_for_query()
            throws Exception
    {
        List<Object> start;
        List<Object> end;

        start = Arrays.<Object> asList("a", 13);
        end = Arrays.<Object> asList("a", 14);

        exception.expect(AchillesException.class);
        exception
                .expectMessage("For slice query with descending order, start component '13' should be greater or equal to end component '14'");

        validator.validateComponentsForQuery(start, end, DESCENDING);
    }

    @Test
    public void should_validate_compound_keys_for_query() throws Exception
    {
        List<Object> start = Arrays.<Object> asList(11L, "a");
        List<Object> end = Arrays.<Object> asList(11L, "b");
        validator.validateCompoundKeysForClusteredQuery(pm, start, end, ASCENDING);
    }

    @Test
    public void should_exception_when_start_partition_key_null_for_query() throws Exception
    {
        List<Object> start = Arrays.<Object> asList(null, "a");
        List<Object> end = Arrays.<Object> asList(11L, "b");

        exception.expect(AchillesException.class);
        exception
                .expectMessage("Partition key should not be null for start clustering key : [null, a]");

        validator.validateCompoundKeysForClusteredQuery(pm, start, end, ASCENDING);
    }

    @Test
    public void should_exception_when_end_partition_key_null_for_query() throws Exception
    {
        List<Object> start = Arrays.<Object> asList(11L, "a");
        List<Object> end = Arrays.<Object> asList(null, "b");

        exception.expect(AchillesException.class);
        exception
                .expectMessage("Partition key should not be null for end clustering key : [null, b]");

        validator.validateCompoundKeysForClusteredQuery(pm, start, end, ASCENDING);
    }

    @Test
    public void should_exception_when_start_and_end_partition_keys_not_equal_for_query()
            throws Exception
    {
        List<Object> start = Arrays.<Object> asList(11L, "a");
        List<Object> end = Arrays.<Object> asList(12L, "b");

        exception.expect(AchillesException.class);
        exception
                .expectMessage("Partition key should be equal for start and end clustering keys : [[11, a],[12, b]]");

        validator.validateCompoundKeysForClusteredQuery(pm, start, end, ASCENDING);
    }
}
