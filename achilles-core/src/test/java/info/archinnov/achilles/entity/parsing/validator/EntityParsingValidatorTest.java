package info.archinnov.achilles.entity.parsing.validator;

import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ConfigurationContext.Impl;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.google.common.collect.ImmutableMap;

/**
 * AchillesEntityParsingValidatorTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityParsingValidatorTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private EntityParsingValidator validator = new EntityParsingValidator();

    @Test
    public void should_exception_when_no_id_meta() throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("The entity '"
                        + CompleteBean.class.getCanonicalName()
                        + "' should have at least one field with javax.persistence.Id/javax.persistence.EmbeddedId annotation");
        validator.validateHasIdMeta(CompleteBean.class, null);
    }

    @Test
    public void should_exception_when_value_less_property_meta_map() throws Exception {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .type(PropertyType.ID)
                .build();

        EntityParsingContext context = new EntityParsingContext(null, null, CompleteBean.class);
        context.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("id", idMeta));
        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("The entity '"
                        + CompleteBean.class.getCanonicalName()
                        + "' should have at least one field with javax.persistence.Column or javax.persistence.JoinColumn annotations");

        validator.validatePropertyMetas(context, idMeta);
    }

    @Test
    public void should_skip_wide_row_validation_when_not_wide_row_with_thrift_impl() throws Exception {
        EntityParsingContext context = new EntityParsingContext(null, null, CompleteBean.class);
        context.setClusteredEntity(false);
        context.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

        validator.validateClusteredEntities(context);
    }

    @Test
    public void should_skip_wide_row_validation_when_not_wide_row_with_cql_impl() throws Exception {
        ConfigurationContext configContext = new ConfigurationContext();
        configContext.setImpl(Impl.CQL);
        EntityParsingContext context = new EntityParsingContext(null, configContext, CompleteBean.class);

        context.setClusteredEntity(false);
        context.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

        validator.validateClusteredEntities(context);
    }

    @Test
    public void should_skip_wide_row_validation_for_wide_row_but_with_cql_impl() throws Exception {
        ConfigurationContext configContext = new ConfigurationContext();
        configContext.setImpl(Impl.CQL);
        EntityParsingContext context = new EntityParsingContext(null, configContext, CompleteBean.class);

        context.setClusteredEntity(true);
        context.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

        validator.validateClusteredEntities(context);
    }

    @Test
    public void should_exception_when_more_than_two_property_metas_for_wide_row() throws Exception {
        ConfigurationContext configContext = new ConfigurationContext();
        configContext.setImpl(Impl.THRIFT);
        EntityParsingContext context = new EntityParsingContext(null, configContext, CompleteBean.class);
        HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
        propertyMetas.put("name", null);
        propertyMetas.put("age", null);
        propertyMetas.put("id", null);
        context.setPropertyMetas(propertyMetas);
        context.setClusteredEntity(true);

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The clustered entity '" + CompleteBean.class.getCanonicalName()
                + "' should not have more than two properties annotated with @EmbeddedId/@Column/@JoinColumn");

        validator.validateClusteredEntities(context);
    }

    @Test
    public void should_exception_when_no_embedded_id_found_for_wide_row() throws Exception {
        ConfigurationContext configContext = new ConfigurationContext();
        configContext.setImpl(Impl.THRIFT);
        EntityParsingContext context = new EntityParsingContext(null, configContext, CompleteBean.class);
        HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();

        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
                .valueClass(Long.class).type(PropertyType.ID).build();

        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
                .valueClass(String.class).type(PropertyType.SIMPLE).build();
        propertyMetas.put("id", idMeta);
        propertyMetas.put("name", propertyMeta);
        context.setPropertyMetas(propertyMetas);
        context.setClusteredEntity(true);

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The clustered entity '" + CompleteBean.class.getCanonicalName()
                + "' should have an @EmbeddedId");

        validator.validateClusteredEntities(context);
    }

    @Test
    public void should_exception_when_incorrect_clustered_value_type_for_wide_row() throws Exception {
        ConfigurationContext configContext = new ConfigurationContext();
        configContext.setImpl(Impl.THRIFT);
        EntityParsingContext context = new EntityParsingContext(null, configContext, CompleteBean.class);
        HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();

        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
                .valueClass(Long.class).type(PropertyType.EMBEDDED_ID).build();

        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
                .valueClass(String.class).type(PropertyType.LIST).build();
        propertyMetas.put("id", idMeta);
        propertyMetas.put("name", propertyMeta);
        context.setPropertyMetas(propertyMetas);
        context.setClusteredEntity(true);

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The clustered entity '" + CompleteBean.class.getCanonicalName()
                + "' should have a single @Column/@JoinColumn property of type simple/join simple/counter");

        validator.validateClusteredEntities(context);
    }

    @Test
    public void should_exception_when_join_entity_is_wide_row() throws Exception {
        PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
                .valueClass(String.class).field("test").entityClassName("entity").build();

        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setClusteredEntity(true);
        joinMeta.setClassName("class.name");
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The entity 'class.name' is a clustered entity and cannot be a join entity");

        validator.validateJoinEntityNotClusteredEntity(propertyMeta, joinMeta);
    }

    @Test
    public void should_exception_when_join_entity_does_not_exist_in_properties_map() throws Exception {
        Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();
        entityMetaMap.put(this.getClass(), null);

        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("Cannot find mapping for join entity '" + CompleteBean.class.getCanonicalName() + "'");

        validator.validateJoinEntityExist(entityMetaMap, CompleteBean.class);

    }

    @Test
    public void should_exception_when_no_entity_found_after_parsing() throws Exception {
        List<Class<?>> entities = new ArrayList<Class<?>>();
        List<String> entityPackages = Arrays.asList("com.package1", "com.package2");

        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("No entity with javax.persistence.Entity/javax.persistence.Table annotations found in the packages 'com.package1,com.package2'");

        validator.validateAtLeastOneEntity(entities, entityPackages);

    }
}
