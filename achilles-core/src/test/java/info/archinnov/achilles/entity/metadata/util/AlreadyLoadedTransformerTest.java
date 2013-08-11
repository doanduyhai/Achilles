package info.archinnov.achilles.entity.metadata.util;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.collect.FluentIterable;

/**
 * AlreadyLoadedTransformerTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class AlreadyLoadedTransformerTest
{

    @Test
    public void should_transform() throws Exception
    {
        Map<Method, PropertyMeta> getterMetas = new HashMap<Method, PropertyMeta>();
        AlreadyLoadedTransformer transformer = new AlreadyLoadedTransformer(getterMetas);

        PropertyMeta pm1 = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .accessors()
                .type(PropertyType.SIMPLE)
                .build();

        getterMetas.put(pm1.getGetter(), pm1);

        List<PropertyMeta> list = FluentIterable
                .from(Arrays.asList(pm1.getGetter()))
                .transform(transformer)
                .toImmutableList();

        assertThat(list).containsExactly(pm1);
    }
}
