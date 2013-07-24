package info.archinnov.achilles.compound;

import static info.archinnov.achilles.type.OrderingMode.*;
import info.archinnov.achilles.exception.AchillesException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * CQLCompoundKeyValidatorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLCompoundKeyValidatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private CQLCompoundKeyValidator validator = new CQLCompoundKeyValidator();

    @Test
    public void should_validate_single_key() throws Exception {
        validator.validateComponentsForSliceQuery(Arrays.<Object> asList(10L),
                Arrays.<Object> asList(11L), ASCENDING);
    }

    @Test
    public void should_exception_when_single_key_not_in_correct_order() throws Exception {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("For slice query with ascending order, start clustering last component should be 'lesser or equal' to end clustering last component: [[11],[10]");
        validator.validateComponentsForSliceQuery(Arrays.<Object> asList(11L), Arrays.<Object> asList(10L),
                ASCENDING);
    }

    @Test
    public void should_exception_when_components_not_in_correct_order_for_ascending() throws Exception {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("For slice query with ascending order, start clustering last component should be 'lesser or equal' to end clustering last component: [[10,11],[10,10]");
        validator.validateComponentsForSliceQuery(Arrays.<Object> asList(10L, 11), Arrays.<Object> asList(10L, 10),
                ASCENDING);
    }

    @Test
    public void should_exception_when_components_not_in_correct_order_for_descending() throws Exception {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("For slice query with descending order, start clustering last component should be 'greater or equal' to end clustering last component: [[10,11],[10,12]");
        validator.validateComponentsForSliceQuery(Arrays.<Object> asList(10L, 11), Arrays.<Object> asList(10L, 12),
                DESCENDING);
    }

    @Test
    public void should_validate_clustering_keys() throws Exception {
        UUID uuid1 = new UUID(10, 11);
        UUID uuid2 = new UUID(10, 12);

        List<Object> start = Arrays.<Object> asList(10L, uuid1, "author", 3);
        List<Object> end = Arrays.<Object> asList(10L, uuid1, "author", 4);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList(10L, uuid1, "author", 3);
        end = Arrays.<Object> asList(10L, uuid1, "author", null);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList(10L, uuid1, "author", null);
        end = Arrays.<Object> asList(10L, uuid1, "author", 3);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList(10L, uuid1, "a", null);
        end = Arrays.<Object> asList(10L, uuid1, "b", null);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList(10L, uuid1, "a", null);
        end = Arrays.<Object> asList(10L, uuid1, null, null);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList(10L, uuid1, null, null);
        end = Arrays.<Object> asList(10L, uuid1, "b", null);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList(10L, uuid1, null, null);
        end = Arrays.<Object> asList(10L, uuid2, null, null);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList(10L, uuid1, "a", null);
        end = Arrays.<Object> asList(10L, uuid1, "a", null);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList(10L);
        end = Arrays.<Object> asList(10L);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);
        validator.validateComponentsForSliceQuery(start, new ArrayList<Object>(), ASCENDING);
        validator.validateComponentsForSliceQuery(new ArrayList<Object>(), end, ASCENDING);
    }

    @Test
    public void should_exception_when_too_many_start_components() throws Exception {
        UUID uuid1 = new UUID(10, 11);

        List<Object> start = Arrays.<Object> asList(uuid1, "a", 1);
        List<Object> end = Arrays.<Object> asList(uuid1, null, null);

        exception.expect(AchillesException.class);
        exception.expectMessage("There should be no more than 1 component difference between clustering keys: [["
                + uuid1 + ",a,1],[" + uuid1 + ",,]");
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);
    }

    @Test
    public void should_exception_when_components_are_not_equal_case1() throws Exception {
        UUID uuid1 = new UUID(10, 11);
        List<Object> start = Arrays.<Object> asList(uuid1, "a", 1);
        List<Object> end = Arrays.<Object> asList(uuid1, "b", 2);

        exception.expect(AchillesException.class);
        exception.expectMessage("2th component for clustering keys should be equal: [[" + uuid1 + ",a,1],[" + uuid1
                + ",b,2]");
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);
    }

    @Test
    public void should_exception_when_components_are_not_equal_case2() throws Exception {
        UUID uuid1 = new UUID(10, 11);
        List<Object> start = Arrays.<Object> asList(uuid1, "a", null);
        List<Object> end = Arrays.<Object> asList(uuid1, "b", 2);

        exception.expect(AchillesException.class);
        exception.expectMessage("2th component for clustering keys should be equal: [[" + uuid1 + ",a,],[" + uuid1
                + ",b,2]");
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);
    }

}
