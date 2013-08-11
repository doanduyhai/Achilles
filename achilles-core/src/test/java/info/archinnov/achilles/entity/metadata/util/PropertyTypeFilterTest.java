package info.archinnov.achilles.entity.metadata.util;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import java.util.Arrays;
import org.junit.Test;
import com.google.common.collect.Collections2;

/**
 * PropertyTypeFilterTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyTypeFilterTest
{

    @Test
    public void should_filter_by_types() throws Exception
    {
        PropertyTypeFilter filter = new PropertyTypeFilter(COUNTER, SIMPLE);

        PropertyMeta pm1 = PropertyMetaTestBuilder
                .valueClass(String.class)
                .entityClassName("entity")
                .field("pm1")
                .type(SET)
                .build();

        PropertyMeta pm2 = PropertyMetaTestBuilder
                .valueClass(String.class)
                .entityClassName("entity")
                .field("pm2")
                .type(SIMPLE)
                .build();

        PropertyMeta pm3 = PropertyMetaTestBuilder
                .valueClass(String.class)
                .entityClassName("entity")
                .field("pm3")
                .type(MAP)
                .build();

        assertThat(Collections2.filter(Arrays.asList(pm1, pm2), filter)).containsOnly(pm2);
        assertThat(Collections2.filter(Arrays.asList(pm1, pm3), filter)).isEmpty();
    }
}
