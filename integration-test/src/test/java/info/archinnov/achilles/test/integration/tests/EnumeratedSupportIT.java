package info.archinnov.achilles.test.integration.tests;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.EntityWithEnumeratedConfig;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.TypedMap;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.test.integration.entity.EntityWithEnumeratedConfig.TABLE_NAME;
import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.lang.annotation.RetentionPolicy.SOURCE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.data.MapEntry.entry;

/**
 * Test case for bug #108: Slice query: partition components and clustering keys should be encoded properly
 */
public class EnumeratedSupportIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(
            Steps.BOTH, TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();



    @Test
    public void should_encode_and_decode_enum_correctly() {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final List<ElementType> elementTypes = asList(FIELD, METHOD);
        final ImmutableMap<RetentionPolicy, ElementType> retentionPolicies = ImmutableMap.of(SOURCE, ANNOTATION_TYPE, RUNTIME, CONSTRUCTOR);

        manager.insert(new EntityWithEnumeratedConfig(id, ConsistencyLevel.LOCAL_ONE, elementTypes, retentionPolicies));

        final EntityWithEnumeratedConfig found = manager.find(EntityWithEnumeratedConfig.class, id);
        assertThat(found.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
        assertThat(found.getElementTypes()).containsExactly(FIELD, METHOD);
        assertThat(found.getRetentionPolicies()).contains(entry(SOURCE, ANNOTATION_TYPE), entry(RUNTIME, CONSTRUCTOR));
    }

    @Test
    public void should_encode_enum_correctly() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final List<ElementType> elementTypes = asList(FIELD, METHOD);
        final ImmutableMap<RetentionPolicy, ElementType> retentionPolicies = ImmutableMap.of(SOURCE, ANNOTATION_TYPE, RUNTIME, CONSTRUCTOR);

        manager.insert(new EntityWithEnumeratedConfig(id, ConsistencyLevel.LOCAL_ONE, elementTypes, retentionPolicies));

        //When
        final Where statement = select().from(TABLE_NAME).where(eq("id", bindMarker("id")));

        final TypedMap found = manager.nativeQuery(statement, id).getFirst();

        //Then
        assertThat(found.getTyped("id")).isEqualTo(id);
        assertThat(found.getTyped("consistency_level")).isEqualTo(ConsistencyLevel.LOCAL_ONE.ordinal());
        assertThat(found.<List<String>>getTyped("element_types")).containsExactly(FIELD.name(), METHOD.name());
        assertThat(found.<Map<Integer,String>>getTyped("retention_policies")).contains(entry(0, ANNOTATION_TYPE.name()), entry(2, CONSTRUCTOR.name()));
    }

    @Test
    public void should_decode_enum_correctly() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final Insert insert = insertInto(TABLE_NAME).value("id", id)
                .value("consistency_level", ConsistencyLevel.LOCAL_ONE.ordinal())
                .value("element_types", asList(FIELD.name(), METHOD.name()))
                .value("retention_policies", ImmutableMap.of(0, ANNOTATION_TYPE.name(), 2, CONSTRUCTOR.name()));

        manager.nativeQuery(insert).execute();

        //When
        final EntityWithEnumeratedConfig found = manager.find(EntityWithEnumeratedConfig.class, id);

        //Then
        assertThat(found.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
        assertThat(found.getElementTypes()).containsExactly(FIELD, METHOD);
        assertThat(found.getRetentionPolicies()).contains(entry(SOURCE, ANNOTATION_TYPE), entry(RUNTIME, CONSTRUCTOR));
    }
}
