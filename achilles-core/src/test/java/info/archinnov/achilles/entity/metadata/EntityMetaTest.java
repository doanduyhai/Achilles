package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * EntityMetaTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMetaTest
{
    @Test
    public void should_to_string() throws Exception
    {
        Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
        propertyMetas.put("name", null);
        propertyMetas.put("age", null);

        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, Long.class)
                .field("id")
                .type(PropertyType.SIMPLE)
                .consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ALL, ALL))
                .build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClassName("className");
        entityMeta.setTableName("cfName");
        entityMeta.setIdClass(Long.class);
        entityMeta.setPropertyMetas(propertyMetas);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setClusteredEntity(true);
        entityMeta.setConsistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ONE, ONE));

        StringBuilder toString = new StringBuilder();
        toString.append("EntityMeta [className=className, ");
        toString.append("columnFamilyName=cfName, ");
        toString.append("propertyMetas=[age,name], ");
        toString.append("idMeta=").append(idMeta.toString()).append(", ");
        toString.append("clusteredEntity=true, ");
        toString.append("consistencyLevels=[ONE,ONE]]");
        assertThat(entityMeta.toString()).isEqualTo(toString.toString());
    }

    @Test
    public void should_get_cql_table_name() throws Exception
    {
        EntityMeta meta = new EntityMeta();
        meta.setTableName("TaBle");

        assertThat(meta.getCQLTableName()).isEqualTo("table");
    }

    @Test
    public void should_get_all_metas() throws Exception {

        PropertyMeta<?, ?> pm1 = new PropertyMeta<Void, String>();
        PropertyMeta<?, ?> pm2 = new PropertyMeta<Void, String>();

        Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
        propertyMetas.put("name", pm1);
        propertyMetas.put("age", pm2);

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setPropertyMetas(propertyMetas);

        assertThat(entityMeta.getAllMetas()).containsExactly(pm1, pm2);
    }

    @Test
    public void should_get_all_metas_except_id_meta() throws Exception {

        PropertyMeta<?, ?> pm1 = new PropertyMeta<Void, String>();
        pm1.setType(SIMPLE);
        PropertyMeta<?, ?> pm2 = new PropertyMeta<Void, String>();
        pm2.setType(ID);

        Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
        propertyMetas.put("name", pm1);
        propertyMetas.put("age", pm2);

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setPropertyMetas(propertyMetas);

        assertThat(entityMeta.getAllMetasExceptIdMeta()).containsExactly(pm1);
    }
}
