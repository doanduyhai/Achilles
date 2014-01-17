package info.archinnov.achilles.internal.metadata.parsing.context;

import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyParsingContextTest {

    @Test
    public void should_set_primaryKey_to_true_when_embedded_id() throws Exception {
        //Given
        PropertyParsingContext context = new PropertyParsingContext(null,null);

        //When
        context.setEmbeddedId(true);

        //Then
        assertThat(context.isEmbeddedId()).isTrue();
        assertThat(context.isPrimaryKey()).isTrue();
    }
}
