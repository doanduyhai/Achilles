package info.archinnov.achilles.junit;

import static info.archinnov.achilles.junit.AchillesTestResource.Steps.BOTH;
import org.junit.rules.ExternalResource;

/**
 * AchillesEmbeddedCassandra
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class AchillesTestResource extends ExternalResource {

    protected final String[] tables;
    private Steps steps = BOTH;

    public AchillesTestResource(String... tables) {
        this.tables = tables;
    }

    public AchillesTestResource(Steps cleanUpSteps, String... tables) {
        this.steps = cleanUpSteps;
        this.tables = tables;
    }

    protected void before() throws Throwable {
        if (steps.isBefore())
            truncateTables();
    }

    protected void after() {
        if (steps.isAfter())
            truncateTables();
    }

    protected abstract void truncateTables();

    public static enum Steps
    {
        BEFORE_TEST, AFTER_TEST, BOTH;

        public boolean isBefore()
        {
            return (this == BOTH || this == BEFORE_TEST);
        }

        public boolean isAfter()
        {
            return (this == BOTH || this == AFTER_TEST);
        }
    }
}
