package info.archinnov.achilles.entity.parsing.context;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyParsingContextTest {

    @Test
    public void should_set_primaryKey_to_true_when_embedded_id() throws Exception {
        //Given
        PropertyParsingContext context = new PropertyParsingContext(null,null);

        //When
        context.isEmbeddedId(true);

        //Then
        assertThat(context.isEmbeddedId()).isTrue();
        assertThat(context.isPrimaryKey()).isTrue();
    }
}
