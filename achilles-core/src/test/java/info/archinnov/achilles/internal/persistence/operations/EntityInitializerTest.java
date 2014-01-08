package info.archinnov.achilles.internal.persistence.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;

@RunWith(MockitoJUnitRunner.class)
public class EntityInitializerTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private EntityInitializer initializer = new EntityInitializer();


    @Mock
    private EntityMeta meta;

    @Mock
    private PropertyMeta counterMeta;

    private CompleteBean bean = new CompleteBean();


    @Test
    public void should_initialize_and_set_counter_value_for_entity() throws Exception {

        when(meta.getAllCounterMetas()).thenReturn(Arrays.asList(counterMeta));

        initializer.initializeEntity(bean, meta);

        verify(counterMeta).invokeGetter(bean);

    }


}
