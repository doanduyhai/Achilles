package fr.doan.achilles.metadata.builder;

import static fr.doan.achilles.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.SimplePropertyMeta;
import fr.doan.achilles.serializer.Utils;

@RunWith(MockitoJUnitRunner.class)
public class EntityMetaBuilderTest {

    @Mock
    private ExecutingKeyspace keyspace;

    @Mock
    private GenericDao<?> dao;

    @Mock
    private PropertyMeta<Long> idMeta;

    @SuppressWarnings("unchecked")
    @Test
    public void should_build_meta() throws Exception {

        Map<String, PropertyMeta<?>> propertyMetas = new HashMap<String, PropertyMeta<?>>();
        SimplePropertyMeta<String> simpleMeta = new SimplePropertyMeta<String>();
        propertyMetas.put("name", simpleMeta);

        when(idMeta.getValueClass()).thenReturn(Long.class);

        EntityMeta<Long> meta = entityMetaBuilder(idMeta).canonicalClassName("fr.doan.Bean").serialVersionUID(1L)
                .propertyMetas(propertyMetas).keyspace(keyspace).build();

        assertThat(meta.getCanonicalClassName()).isEqualTo("fr.doan.Bean");
        assertThat(meta.getColumnFamilyName()).isEqualTo("fr_doan_Bean");
        assertThat(meta.getIdMeta()).isSameAs(idMeta);
        assertThat(meta.getIdSerializer().getComparatorType()).isEqualTo(Utils.LONG_SRZ.getComparatorType());
        assertThat(meta.getPropertyMetas()).containsKey("name");
        assertThat(meta.getPropertyMetas()).containsValue(simpleMeta);
        assertThat(meta.getDao()).isNotNull();

    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_build_meta_with_column_family_name() throws Exception {

        Map<String, PropertyMeta<?>> propertyMetas = new HashMap<String, PropertyMeta<?>>();
        SimplePropertyMeta<String> simpleMeta = new SimplePropertyMeta<String>();
        propertyMetas.put("name", simpleMeta);

        when(idMeta.getValueClass()).thenReturn(Long.class);

        EntityMeta<Long> meta = entityMetaBuilder(idMeta).canonicalClassName("fr.doan.Bean").serialVersionUID(1L)
                .propertyMetas(propertyMetas).columnFamilyName("toto").keyspace(keyspace).build();

        assertThat(meta.getCanonicalClassName()).isEqualTo("fr.doan.Bean");
        assertThat(meta.getColumnFamilyName()).isEqualTo("toto");

    }

    class Bean implements Serializable {

        private static final long serialVersionUID = 1L;

        private Long id;

        private String name;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

    }
}
