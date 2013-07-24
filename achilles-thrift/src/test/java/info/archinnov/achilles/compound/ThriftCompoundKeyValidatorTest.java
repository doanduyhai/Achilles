package info.archinnov.achilles.compound;

import static info.archinnov.achilles.type.OrderingMode.*;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * ThriftCompoundKeyValidatorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftCompoundKeyValidatorTest {

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
    public void should_validate_components_for_query_in_ascending_order_for_query()
            throws Exception
    {
        List<Object> start;
        List<Object> end;

        start = Arrays.<Object> asList("a", 13);
        end = Arrays.<Object> asList("a", 13);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", 13);
        end = Arrays.<Object> asList("a", 14);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        // ///////////////
        start = Arrays.<Object> asList("a", 13);
        end = Arrays.<Object> asList("b", 13);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", 13);
        end = Arrays.<Object> asList("b", 14);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", 14);
        end = Arrays.<Object> asList("b", 13);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        // //////////////
        start = Arrays.<Object> asList("a", null);
        end = Arrays.<Object> asList("a", 14);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", 14);
        end = Arrays.<Object> asList("a", null);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        // /////////////////
        start = Arrays.<Object> asList("a", null);
        end = Arrays.<Object> asList("b", null);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", null);
        end = Arrays.<Object> asList("b", 14);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

        start = Arrays.<Object> asList("a", 14);
        end = Arrays.<Object> asList("b", null);
        validator.validateComponentsForSliceQuery(start, end, ASCENDING);
    }

    @Test
    public void should_validate_components_for_query_in_descending_order_for_query()
            throws Exception
    {
        List<Object> start;
        List<Object> end;

        start = Arrays.<Object> asList("a", 13);
        end = Arrays.<Object> asList("a", 13);
        validator.validateComponentsForSliceQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("a", 14);
        end = Arrays.<Object> asList("a", 13);
        validator.validateComponentsForSliceQuery(start, end, DESCENDING);

        // ///////////////
        start = Arrays.<Object> asList("b", 13);
        end = Arrays.<Object> asList("a", 13);
        validator.validateComponentsForSliceQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("b", 13);
        end = Arrays.<Object> asList("a", 14);
        validator.validateComponentsForSliceQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("b", 14);
        end = Arrays.<Object> asList("a", 13);
        validator.validateComponentsForSliceQuery(start, end, DESCENDING);

        // //////////////
        start = Arrays.<Object> asList("a", null);
        end = Arrays.<Object> asList("a", 14);
        validator.validateComponentsForSliceQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("a", 14);
        end = Arrays.<Object> asList("a", null);
        validator.validateComponentsForSliceQuery(start, end, DESCENDING);

        // //////////////////
        start = Arrays.<Object> asList("b", null);
        end = Arrays.<Object> asList("a", null);
        validator.validateComponentsForSliceQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("b", null);
        end = Arrays.<Object> asList("a", 14);
        validator.validateComponentsForSliceQuery(start, end, DESCENDING);

        start = Arrays.<Object> asList("b", 14);
        end = Arrays.<Object> asList("a", null);
        validator.validateComponentsForSliceQuery(start, end, DESCENDING);
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

        validator.validateComponentsForSliceQuery(start, end, ASCENDING);

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

        validator.validateComponentsForSliceQuery(start, end, DESCENDING);
    }

    @Test
    public void should_validate_compound_keys_for_query() throws Exception
    {
        List<Object> start = Arrays.<Object> asList(11L, "a");
        List<Object> end = Arrays.<Object> asList(11L, "b");
        validator.validateComponentsForSliceQuery(pm, start, end, ASCENDING);
    }

    //////////////////////////////////////
    @Test
    public void should_validate_single_keys_ascending() throws Exception
    {
        when(pm.isCompound()).thenReturn(false);

        validator.validateBoundsForQuery(pm, 10L, 11L, ASCENDING);
    }

    @Test
    public void should_validate_single_key_descending() throws Exception
    {
        when(pm.isCompound()).thenReturn(false);

        validator.validateBoundsForQuery(pm, 12L, 11L, DESCENDING);
    }

    @Test
    public void should_validate_compound_keys() throws Exception
    {
        when(pm.isCompound()).thenReturn(true);

        CompoundKey start = new CompoundKey();
        CompoundKey end = new CompoundKey();

        List<Method> getters = Arrays.<Method> asList();
        List<Object> startComps = Arrays.<Object> asList();
        List<Object> endComps = Arrays.<Object> asList();

        when(pm.getComponentGetters()).thenReturn(getters);
        when(pm.encodeToComponents(start)).thenReturn(startComps);
        when(pm.encodeToComponents(end)).thenReturn(endComps);

        validator.validateBoundsForQuery(pm, start, end, ASCENDING);

    }

    @Test
    public void should_validate_when_any_key_null() throws Exception
    {
        validator.validateBoundsForQuery(pm, null, 11L, ASCENDING);
        validator.validateBoundsForQuery(pm, 11L, null, ASCENDING);
        validator.validateBoundsForQuery(pm, null, null, ASCENDING);
    }
}
