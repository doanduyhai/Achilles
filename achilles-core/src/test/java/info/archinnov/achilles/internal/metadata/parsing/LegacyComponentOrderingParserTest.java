package info.archinnov.achilles.internal.metadata.parsing;

import static info.archinnov.achilles.type.NamingStrategy.CASE_SENSITIVE;
import static org.fest.assertions.api.Assertions.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.annotations.ClusteringColumn;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.schemabuilder.Create;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class LegacyComponentOrderingParserTest {

    @InjectMocks
    private LegacyComponentOrderingParser parser;

    @Mock
    private PropertyParsingContext context;

    @Mock
    private EntityIntrospector introspector;

    @Before
    public void setUp() {
        parser.introspector = introspector;
        when(context.getClassNamingStrategy()).thenReturn(CASE_SENSITIVE);
    }

    @Test
    public void should_parse_compound_pk() throws Exception {
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
        final Map<Integer, Field> componentsOrdering = parser.extractComponentsOrdering(Test.class);

        //Then
        assertThat(componentsOrdering).hasSize(3);
        assertThat(componentsOrdering.get(1).getName()).isEqualTo("userId");
        assertThat(componentsOrdering.get(2).getName()).isEqualTo("time");
        assertThat(componentsOrdering.get(3).getName()).isEqualTo("type");
    }

    @Test
    public void should_parse_composite_partition_key() throws Exception {
        //Given
        class Test {

            @PartitionKey
            @Order(1)
            private Long userId;

            @PartitionKey
            @Order(2)
            private String type;

            @Order(3)
            private Date time;
        }
        //When
        final Map<Integer, Field> componentsOrdering = parser.extractComponentsOrdering(Test.class);

        //Then
        assertThat(componentsOrdering).hasSize(3);
        assertThat(componentsOrdering.get(1).getName()).isEqualTo("userId");
        assertThat(componentsOrdering.get(2).getName()).isEqualTo("type");
        assertThat(componentsOrdering.get(3).getName()).isEqualTo("time");

    }

    @Test
    public void should_parse_composite_partition_key_only() throws Exception {
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
        final Map<Integer, Field> componentsOrdering = parser.extractComponentsOrdering(Test.class);

        //Then
        assertThat(componentsOrdering).hasSize(2);
        assertThat(componentsOrdering.get(1).getName()).isEqualTo("userId");
        assertThat(componentsOrdering.get(2).getName()).isEqualTo("type");
    }


    @Test(expected = AchillesBeanMappingException.class)
    public void should_exception_when_hole_in_partition_key() throws Exception {
        //Given
        class Test {

            @PartitionKey
            @Order(1)
            private Long userId;

            @PartitionKey
            @Order(3)
            private String type;

            @Order(4)
            private Date time;
        }
        //When
        parser.extractComponentsOrdering(Test.class);
    }

    @Test(expected = AchillesBeanMappingException.class)
    public void should_exception_when_duplicated_partition_key_order() throws Exception {
        //Given
        class Test {

            @PartitionKey
            @Order(1)
            private Long userId;

            @PartitionKey
            @Order(1)
            private String type;

            @Order(2)
            private Date time;
        }
        //When
        parser.extractComponentsOrdering(Test.class);
    }

    @Test(expected = AchillesBeanMappingException.class)
    public void should_exception_when_hole_in_clustering_column() throws Exception {
        //Given
        class Test {
            @PartitionKey
            @Order(1)
            private Long userId;

            @Order(2)
            private Date time;

            @Order(4)
            private String type;
        }
        //When
        parser.extractComponentsOrdering(Test.class);
    }

    @Test(expected = AchillesBeanMappingException.class)
    public void should_exception_when_duplicated_clustering_column_order() throws Exception {
        //Given
        class Test {
            @PartitionKey
            @Order(1)
            private Long userId;

            @Order(2)
            private Date time;

            @Order(2)
            private String type;
        }
        //When
        parser.extractComponentsOrdering(Test.class);
    }

    @Test
    public void should_extract_clustering_order() throws Exception {
        //Given
        class Test {
            @PartitionKey
            @Order(1)
            private Long userId;

            @Order(value = 2, reversed = true)
            private Date time;

            @Order(3)
            private String type;

            @Order(value = 4, reversed = true)
            private int count;
        }

        when(introspector.inferCQLColumnName(Test.class.getDeclaredField("time"), CASE_SENSITIVE)).thenReturn("time");
        when(introspector.inferCQLColumnName(Test.class.getDeclaredField("type"), CASE_SENSITIVE)).thenReturn("type");
        when(introspector.inferCQLColumnName(Test.class.getDeclaredField("count"), CASE_SENSITIVE)).thenReturn("count");

        //When
        final List<Create.Options.ClusteringOrder> clusteringOrders = parser.extractClusteringOrder(Test.class);

        //Then
        assertThat(clusteringOrders).hasSize(3);
        assertThat(clusteringOrders.get(0).getClusteringColumnName()).isEqualTo("time");
        assertThat(clusteringOrders.get(0).getSorting()).isEqualTo(Create.Options.ClusteringOrder.Sorting.DESC);

        assertThat(clusteringOrders.get(1).getClusteringColumnName()).isEqualTo("type");
        assertThat(clusteringOrders.get(1).getSorting()).isEqualTo(Create.Options.ClusteringOrder.Sorting.ASC);

        assertThat(clusteringOrders.get(2).getClusteringColumnName()).isEqualTo("count");
        assertThat(clusteringOrders.get(2).getSorting()).isEqualTo(Create.Options.ClusteringOrder.Sorting.DESC);
    }
}