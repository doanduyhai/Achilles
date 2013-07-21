package info.archinnov.achilles.query;

import static info.archinnov.achilles.type.OrderingMode.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.compound.ThriftCompoundKeyValidator;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * WideMapQueryValidatorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftQueryValidatorTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private ThriftQueryValidator validator;

    @Mock
    private ThriftCompoundKeyValidator compoundKeyValidator;

    @Mock
    private ThriftCompoundKeyMapper mapper;

    @Mock
    private PropertyMeta<?, ?> pm;

    @Captor
    private ArgumentCaptor<List<Method>> gettersCaptor;

    @Test
    public void should_validate_single_keys_ascending() throws Exception
    {
        when(pm.isSingleKey()).thenReturn(true);

        validator.validateBoundsForQuery(pm, 10L, 11L, ASCENDING);
    }

    @Test
    public void should_validate_single_keys_descending() throws Exception
    {
        when(pm.isSingleKey()).thenReturn(true);

        validator.validateBoundsForQuery(pm, 12L, 11L, DESCENDING);
    }

    @Test
    public void should_validate_compound_keys() throws Exception
    {
        when(pm.isSingleKey()).thenReturn(false);

        CompoundKey start = new CompoundKey();
        CompoundKey end = new CompoundKey();

        List<Method> getters = Arrays.<Method> asList();
        List<Object> startComps = Arrays.<Object> asList();
        List<Object> endComps = Arrays.<Object> asList();

        when(pm.getComponentGetters()).thenReturn(getters);
        when(mapper.fromCompoundToComponents(start, getters)).thenReturn(startComps);
        when(mapper.fromCompoundToComponents(end, getters)).thenReturn(endComps);

        validator.validateBoundsForQuery(pm, start, end, ASCENDING);

        verify(compoundKeyValidator).validateComponentsForQuery(startComps, endComps, ASCENDING);
    }

    @Test
    public void should_validate_when_any_key_null() throws Exception
    {
        validator.validateBoundsForQuery(pm, null, 11L, ASCENDING);
        validator.validateBoundsForQuery(pm, 11L, null, ASCENDING);
        validator.validateBoundsForQuery(pm, null, null, ASCENDING);
    }

}
