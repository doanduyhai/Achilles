package info.archinnov.achilles.internal.table;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.sun.org.apache.bcel.internal.generic.RET;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class SchemaReaderTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Cluster cluster;

    private SchemaReader reader = new SchemaReader();

    @Test
    public void should_fetch_table_meta_from_different_keyspaces() throws Exception {
        //Given
        EntityMeta meta1 = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        EntityMeta meta2 = mock(EntityMeta.class, RETURNS_DEEP_STUBS);

        KeyspaceMetadata ks1Meta = mock(KeyspaceMetadata.class, RETURNS_DEEP_STUBS);
        KeyspaceMetadata ks2Meta = mock(KeyspaceMetadata.class, RETURNS_DEEP_STUBS);

        TableMetadata tableMeta1 = mock(TableMetadata.class, RETURNS_DEEP_STUBS);
        TableMetadata tableMeta2 = mock(TableMetadata.class, RETURNS_DEEP_STUBS);

        when(meta1.config().getKeyspaceName()).thenReturn("ks1");
        when(meta1.config().getTableName()).thenReturn("table1");
        when(meta1.config().getQualifiedTableName()).thenReturn("ks1.table1");
        when(meta2.config().getKeyspaceName()).thenReturn("ks2");
        when(meta2.config().getTableName()).thenReturn("table2");
        when(meta2.config().getQualifiedTableName()).thenReturn("ks2.table2");

        when(cluster.getMetadata().getKeyspace("ks1")).thenReturn(ks1Meta);
        when(cluster.getMetadata().getKeyspace("ks2")).thenReturn(ks2Meta);

        when(ks1Meta.getTable("table1")).thenReturn(tableMeta1);
        when(ks2Meta.getTable("table2")).thenReturn(tableMeta2);

        //When
        final Map<String, TableMetadata> tableMetaData = reader.fetchTableMetaData(cluster, Arrays.asList(meta1, meta2));

        //Then
        assertThat(tableMetaData.get("ks1.table1")).isSameAs(tableMeta1);
        assertThat(tableMetaData.get("ks2.table2")).isSameAs(tableMeta2);
    }

    @Test
        public void should_exception_when_keyspace_meta_is_not_found() throws Exception {
        //Given
        EntityMeta meta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        when(meta.config().getKeyspaceName()).thenReturn("ks");
        when(meta.config().getTableName()).thenReturn("table");

        when(cluster.getMetadata().getKeyspace("ks")).thenReturn(null);

        //When
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("Keyspace 'ks' doest not exist or cannot be found");

        reader.fetchTableMetaData(cluster, Arrays.asList(meta));
    }

    @Test
    public void should_return_empty_map_when_table_meta_is_not_found() throws Exception {
        //Given
        EntityMeta meta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        KeyspaceMetadata ksMeta = mock(KeyspaceMetadata.class, RETURNS_DEEP_STUBS);

        when(meta.config().getKeyspaceName()).thenReturn("ks");
        when(meta.config().getTableName()).thenReturn("table");
        when(meta.config().getQualifiedTableName()).thenReturn("ks.table");

        when(cluster.getMetadata().getKeyspace("ks")).thenReturn(ksMeta);
        when(ksMeta.getTable("table")).thenReturn(null);


        //When
        assertThat(reader.fetchTableMetaData(cluster, Arrays.asList(meta))).isEmpty();
    }
}