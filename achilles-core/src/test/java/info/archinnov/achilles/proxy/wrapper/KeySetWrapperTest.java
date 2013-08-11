package info.archinnov.achilles.proxy.wrapper;

import java.util.Arrays;
import java.util.HashSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * AchillesKeySetWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeySetWrapperTest
{

    @Test(expected = UnsupportedOperationException.class)
    public void should_exception_on_add() throws Exception
    {
        KeySetWrapper wrapper = new KeySetWrapper(new HashSet<Object>());

        wrapper.add(5);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void should_exception_on_add_all() throws Exception
    {
        KeySetWrapper wrapper = new KeySetWrapper(new HashSet<Object>());

        wrapper.addAll(Arrays.asList(5, 7));
    }

}
