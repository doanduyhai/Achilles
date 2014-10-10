package info.archinnov.achilles.internal.metadata.parsing;

import info.archinnov.achilles.type.NamingStrategy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class NamingHelperTest {

    @Test
    public void should_render_to_snake_case() throws Exception {
        assertThat(NamingHelper.applyNamingStrategy("count", NamingStrategy.SNAKE_CASE)).isEqualTo("count");
        assertThat(NamingHelper.applyNamingStrategy("name1", NamingStrategy.SNAKE_CASE)).isEqualTo("name_1");
        assertThat(NamingHelper.applyNamingStrategy("firstName", NamingStrategy.SNAKE_CASE)).isEqualTo("first_name");
        assertThat(NamingHelper.applyNamingStrategy("first_name", NamingStrategy.SNAKE_CASE)).isEqualTo("first_name");
    }

    @Test
    public void should_render_to_case_sensitive() throws Exception {
        assertThat(NamingHelper.applyNamingStrategy("count", NamingStrategy.CASE_SENSITIVE)).isEqualTo("count");
        assertThat(NamingHelper.applyNamingStrategy("name1", NamingStrategy.CASE_SENSITIVE)).isEqualTo("name1");
        assertThat(NamingHelper.applyNamingStrategy("firstName", NamingStrategy.CASE_SENSITIVE)).isEqualTo("\"firstName\"");
        assertThat(NamingHelper.applyNamingStrategy("first_name", NamingStrategy.CASE_SENSITIVE)).isEqualTo("first_name");
    }


    @Test
    public void should_render_to_lower_case() throws Exception {
        assertThat(NamingHelper.applyNamingStrategy("count", NamingStrategy.LOWER_CASE)).isEqualTo("count");
        assertThat(NamingHelper.applyNamingStrategy("name1", NamingStrategy.LOWER_CASE)).isEqualTo("name1");
        assertThat(NamingHelper.applyNamingStrategy("firstName", NamingStrategy.LOWER_CASE)).isEqualTo("firstname");
        assertThat(NamingHelper.applyNamingStrategy("first_name", NamingStrategy.LOWER_CASE)).isEqualTo("first_name");
    }

}