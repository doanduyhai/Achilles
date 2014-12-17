package info.archinnov.achilles.internal.metadata.parsing;

import info.archinnov.achilles.annotations.ClusteringColumn;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ComponentOrderingParserTest {

    @Mock
    private PropertyParsingContext context;

    @Test
    public void should_select_new_parser() throws Exception {
        //Given
        class Test {

            @PartitionKey
            private Long userId;

            @ClusteringColumn(1)
            private Date time;

            @ClusteringColumn(2)
            private String type;
        }
        //When
        final ComponentOrderingParser parser = ComponentOrderingParser.determineAppropriateParser(Test.class, context);

        //Then
        assertThat(parser).isInstanceOf(DefaultComponentOrderingParser.class);
    }

    @Test
    public void should_select_new_parser_for_composite_partition_key() throws Exception {
        //Given
        class Test {

            @PartitionKey(1)
            private Long userId;

            @PartitionKey(2)
            private String type;
        }
        //When
        final ComponentOrderingParser parser = ComponentOrderingParser.determineAppropriateParser(Test.class, context);

        //Then
        assertThat(parser).isInstanceOf(DefaultComponentOrderingParser.class);
    }

    @Test
    public void should_select_old_parser() throws Exception {
        //Given
        class Test {

            @PartitionKey
            @Order(1)
            private Long userId;

            @Order(2)
            private Date time;

            @Order(3)
            private String type;
        }
        //When
        final ComponentOrderingParser parser = ComponentOrderingParser.determineAppropriateParser(Test.class, context);

        //Then
        assertThat(parser).isInstanceOf(LegacyComponentOrderingParser.class);
    }

    @Test
    public void should_select_old_parser_for_composite_partition_key() throws Exception {
        //Given
        class Test {

            @PartitionKey
            @Order(1)
            private Long userId;

            @PartitionKey
            @Order(2)
            private String type;
        }
        //When
        final ComponentOrderingParser parser = ComponentOrderingParser.determineAppropriateParser(Test.class, context);

        //Then
        assertThat(parser).isInstanceOf(LegacyComponentOrderingParser.class);
    }

    @Test(expected = AchillesBeanMappingException.class)
    public void should_exception_when_mixing_old_and_new_annotations() throws Exception {
        //Given
        class Test {

            @PartitionKey
            private Long userId;

            @ClusteringColumn(1)
            private Date time;

            @Order(2)
            private String type;
        }
        //When
        ComponentOrderingParser.determineAppropriateParser(Test.class, context);
    }
}
