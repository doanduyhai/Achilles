package info.archinnov.achilles.test.integration.tests.bugs;

import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.exception.AchillesBeanValidationException;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.EntityWithFieldLevelConstraint;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import static info.archinnov.achilles.junit.AchillesTestResource.Steps.BOTH;
import static org.fest.assertions.api.Assertions.assertThat;

public class DisableBeanValidationOnNotSetFieldForProxyIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(BOTH, EntityWithFieldLevelConstraint.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_update_on_proxy_without_failing_on_bean_validation() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);

        final EntityWithFieldLevelConstraint entity = new EntityWithFieldLevelConstraint(id, "name",33);

        manager.insert(entity);

        //When
        final EntityWithFieldLevelConstraint proxy = manager.forUpdate(EntityWithFieldLevelConstraint.class, id);
        proxy.setAge(34);

        manager.update(proxy);

        //Then
        final EntityWithFieldLevelConstraint found = manager.find(EntityWithFieldLevelConstraint.class, id);
        assertThat(found.getAge()).isEqualTo(34);
    }

    @Test(expected = AchillesBeanValidationException.class)
    public void should_raise_bean_validation_exception_on_proxy() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);

        final EntityWithFieldLevelConstraint entity = new EntityWithFieldLevelConstraint(id, "name",33);

        manager.insert(entity);

        //When
        final EntityWithFieldLevelConstraint proxy = manager.forUpdate(EntityWithFieldLevelConstraint.class, id);
        proxy.setName(null);

        manager.update(proxy);
    }
}
