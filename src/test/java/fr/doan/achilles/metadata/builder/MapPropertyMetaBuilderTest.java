package fr.doan.achilles.metadata.builder;

import static fr.doan.achilles.metadata.builder.MapPropertyMetaBuilder.mapPropertyMetaBuilder;
import static org.fest.assertions.api.Assertions.assertThat;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.metadata.MapPropertyMeta;
import fr.doan.achilles.serializer.Utils;

public class MapPropertyMetaBuilderTest {

    @Test
    public void should_build_map_property_meta() throws Exception {

        Method[] accessors = new Method[2];
        accessors[0] = Bean.class.getDeclaredMethod("getPreferences", (Class<?>[]) null);
        accessors[1] = Bean.class.getDeclaredMethod("setPreferences", Map.class);

        MapPropertyMeta<String> meta = (MapPropertyMeta<String>) mapPropertyMetaBuilder(String.class)
                .mapClass(HashMap.class).keyClass(Integer.class).propertyName("preferences").accessors(accessors)
                .build();

        assertThat(meta.getPropertyName()).isEqualTo("preferences");
        assertThat(meta.getValueClass()).isEqualTo(String.class);
        assertThat(meta.getValueSerializer().getComparatorType()).isEqualTo(Utils.STRING_SRZ.getComparatorType());
        assertThat(meta.getGetter()).isEqualTo(accessors[0]);
        assertThat(meta.getSetter()).isEqualTo(accessors[1]);

        assertThat(meta.getKeyClass()).isEqualTo(Integer.class);
        assertThat(meta.getKeySerializer().getComparatorType()).isEqualTo(Utils.INT_SRZ.getComparatorType());
        assertThat(meta.newMapInstance()).isInstanceOf(HashMap.class);

    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_missing_data() throws Exception {

        mapPropertyMetaBuilder(String.class).propertyName("name").build();

    }

    class Bean {
        private Map<Integer, String> preferences;

        public Map<Integer, String> getPreferences() {
            return preferences;
        }

        public void setPreferences(Map<Integer, String> preferences) {
            this.preferences = preferences;
        }
    }
}
