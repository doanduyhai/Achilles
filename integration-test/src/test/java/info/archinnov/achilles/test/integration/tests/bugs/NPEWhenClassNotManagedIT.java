package info.archinnov.achilles.test.integration.tests.bugs;

import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test case for bug #116: Fixes NPE when entity not managed
 */
public class NPEWhenClassNotManagedIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(
            AchillesTestResource.Steps.AFTER_TEST,"CompleteBean");

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test(expected = AchillesException.class)
    public void should_fail_instead_with_AchillesException_when_class_not_managed() {
        manager.sliceQuery(NotManaged.class);
    }

    private static class NotManaged { }
}
