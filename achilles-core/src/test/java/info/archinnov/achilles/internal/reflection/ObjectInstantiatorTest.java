package info.archinnov.achilles.internal.reflection;

import static org.fest.assertions.api.Assertions.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ObjectInstantiatorTest {

    private ObjectInstantiator instantiator = new ObjectInstantiator();

    @Test
    public void should_instantiate_class_without_public_constructor() throws Exception {
        //When
        BeanWithoutPublicConstructor instance = instantiator.instantiate(BeanWithoutPublicConstructor.class);

        //Then
        assertThat(instance).isInstanceOf(BeanWithoutPublicConstructor.class);
    }

    public class BeanWithoutPublicConstructor {
        public BeanWithoutPublicConstructor(String name) {

        }
    }
}