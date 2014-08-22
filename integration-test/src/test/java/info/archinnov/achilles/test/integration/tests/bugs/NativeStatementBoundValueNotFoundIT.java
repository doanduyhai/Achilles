package info.archinnov.achilles.test.integration.tests.bugs;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.persistence.Batch;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ValuelessEntity;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test case for bug #125: BatchedNativeQuery error with multiple statements in batch
 */
public class NativeStatementBoundValueNotFoundIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(
            AchillesTestResource.Steps.AFTER_TEST, ValuelessEntity.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_execute_native_batch() {
        // create persistence manager -- pm
        Batch batch = manager.createBatch();
        batch.startBatch();

        RegularStatement statement = QueryBuilder.insertInto(ValuelessEntity.TABLE_NAME).value("id", 234L);
        batch.batchNativeStatement(statement);

        ValuelessEntity entity = new ValuelessEntity(123L);
        batch.insert(entity);

        batch.endBatch();
    }

}
