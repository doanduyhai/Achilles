package info.archinnov.achilles.junit;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AchillesTestResourceTest {

    private AchillesTestResource resource;

    @Test
    public void should_trigger_before_and_after_when_steps_is_both() throws Throwable
    {
        final StringBuilder witness = new StringBuilder();
        resource = new AchillesTestResource(Steps.BOTH, "table")
        {

            @Override
            protected void truncateTables() {
                witness.append("called");
            }
        };

        resource.before();
        assertThat(witness.toString()).isEqualTo("called");

        witness.delete(0, witness.length());

        resource.after();
        assertThat(witness.toString()).isEqualTo("called");
    }

    @Test
    public void should_trigger_only_before_when_steps_is_before() throws Throwable
    {
        final StringBuilder witness = new StringBuilder();
        resource = new AchillesTestResource(Steps.BEFORE_TEST, "table")
        {

            @Override
            protected void truncateTables() {
                witness.append("called");
            }
        };

        resource.before();
        assertThat(witness.toString()).isEqualTo("called");

        witness.delete(0, witness.length());

        resource.after();
        assertThat(witness.toString()).isEmpty();
    }

    @Test
    public void should_trigger_only_after_when_steps_is_after() throws Throwable
    {
        final StringBuilder witness = new StringBuilder();
        resource = new AchillesTestResource(Steps.AFTER_TEST, "table")
        {

            @Override
            protected void truncateTables() {
                witness.append("called");
            }
        };

        resource.after();
        assertThat(witness.toString()).isEqualTo("called");

        witness.delete(0, witness.length());

        resource.before();
        assertThat(witness.toString()).isEmpty();
    }

}
