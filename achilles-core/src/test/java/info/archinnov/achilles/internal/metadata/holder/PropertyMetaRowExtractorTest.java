package info.archinnov.achilles.internal.metadata.holder;

import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.ColumnDefinitionBuilder;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaRowExtractorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta meta;

    @Mock
    private CompoundPKProperties compoundPKProperties;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Row row;

    private PropertyMetaRowExtractor extractor;

    @Before
    public void setUp() {
        extractor = new PropertyMetaRowExtractor(meta);
        when(meta.getCompoundPKProperties()).thenReturn(compoundPKProperties);
        when(meta.getEntityClassName()).thenReturn("entity");
    }

    @Test
    public void should_extract_raw_compound_pk_components_from_row() throws Exception {
        //Given
        when(compoundPKProperties.getCQLComponentClasses()).thenReturn(Arrays.<Class<?>>asList(Long.class,String.class));
        when(compoundPKProperties.getCQLComponentNames()).thenReturn(asList("id", "name"));

        Definition columnDef1 = ColumnDefinitionBuilder.buildColumnDef("ks", "table", "id", DataType.bigint());
        Definition columnDef2 = ColumnDefinitionBuilder.buildColumnDef("ks", "table", "name", DataType.text());

        when(row.getColumnDefinitions().iterator()).thenReturn(Arrays.asList(columnDef1, columnDef2).iterator());
        when(row.getLong("id")).thenReturn(10L);
        when(row.getString("name")).thenReturn("DuyHai");

        //When
        final List<Object> raws = extractor.extractRawCompoundPrimaryComponentsFromRow(row);

        //Then
        assertThat(raws).containsExactly(10L, "DuyHai");
    }

    @Test
         public void should_validate_extracted_pk_components() throws Exception {
        //Given
        when(compoundPKProperties.getCQLComponentNames()).thenReturn(asList("id", "name"));

        //When
        extractor.validateExtractedCompoundPrimaryComponents(Arrays.<Object>asList(10L, "DuyHai"));

        //Then
    }

    @Test
    public void should_exception_while_validating_extracted_pk_components() throws Exception {
        //Given
        when(compoundPKProperties.getCQLComponentNames()).thenReturn(asList("id", "name"));
        when(meta.<CompleteBean>getValueClass()).thenReturn(CompleteBean.class);

        exception.expect(AchillesException.class);
        exception.expectMessage(String.format("Error, the component '%s' from @CompoundPrimaryKey class 'class %s' is null in Cassandra", "name", CompleteBean.class.getCanonicalName()));

        //When
        extractor.validateExtractedCompoundPrimaryComponents(Arrays.<Object>asList(10L, null));

        //Then
    }

    @Test
    public void should_get_value_on_row() throws Exception {
        //Given
        when(meta.getCQLColumnName()).thenReturn("column");
        when(row.isNull("column")).thenReturn(false);
        when(meta.type()).thenReturn(PropertyType.SIMPLE);
        when(meta.<String>getValueClass()).thenReturn(String.class);

        when(row.getString("column")).thenReturn("a");
        when(meta.forTranscoding().decodeFromCassandra("a")).thenReturn("a");

        //When

        final Object decoded = extractor.invokeOnRowForFields(row);

        //Then
        assertThat(decoded).isEqualTo("a");
    }

    @Test
    public void should_get_list_on_row() throws Exception {
        //Given
        when(meta.getCQLColumnName()).thenReturn("list");
        when(row.isNull("list")).thenReturn(false);
        when(meta.type()).thenReturn(PropertyType.LIST);
        when(meta.<String>getValueClass()).thenReturn(String.class);
        final List<String> rawList = Arrays.asList("a");
        when(row.getList("list",String.class)).thenReturn(rawList);
        when(meta.forTranscoding().decodeFromCassandra(rawList)).thenReturn(rawList);

        //When

        final Object decoded = extractor.invokeOnRowForFields(row);

        //Then
        assertThat(decoded).isSameAs(rawList);
    }

    @Test
    public void should_get_set_on_row() throws Exception {
        //Given
        when(meta.getCQLColumnName()).thenReturn("set");
        when(row.isNull("set")).thenReturn(false);
        when(meta.type()).thenReturn(PropertyType.SET);
        when(meta.<String>getValueClass()).thenReturn(String.class);
        final Set<String> rawSet = Sets.newHashSet("a");
        when(row.getSet("set", String.class)).thenReturn(rawSet);
        when(meta.forTranscoding().decodeFromCassandra(rawSet)).thenReturn(rawSet);

        //When

        final Object decoded = extractor.invokeOnRowForFields(row);

        //Then
        assertThat(decoded).isSameAs(rawSet);
    }

    @Test
    public void should_get_map_on_row() throws Exception {
        //Given
        when(meta.getCQLColumnName()).thenReturn("map");
        when(row.isNull("map")).thenReturn(false);
        when(meta.type()).thenReturn(PropertyType.MAP);
        when(meta.<Integer>getCQLKeyClass()).thenReturn(Integer.class);
        when(meta.<String>getCqlValueClass()).thenReturn(String.class);
        final Map<Integer,String> rawMap = ImmutableMap.of(1,"a");
        when(row.getMap("map", Integer.class, String.class)).thenReturn(rawMap);
        when(meta.forTranscoding().decodeFromCassandra(rawMap)).thenReturn(rawMap);

        //When

        final Object decoded = extractor.invokeOnRowForFields(row);

        //Then
        assertThat(decoded).isSameAs(rawMap);
    }

    @Test
    public void should_get_empty_collection_on_null_from_row() throws Exception {
        //Given
        when(meta.getCQLColumnName()).thenReturn("list");
        when(row.isNull("list")).thenReturn(true);
        when(meta.type()).thenReturn(PropertyType.LIST);
        final List<String> emptyList = Arrays.asList();
        when(meta.forValues().nullValueForCollectionAndMap()).thenReturn(emptyList);

        //When

        final Object decoded = extractor.invokeOnRowForFields(row);

        //Then
        assertThat(decoded).isSameAs(emptyList);
    }

    @Test
    public void should_return_null_when_not_found_in_row() throws Exception {
        //Given
        when(meta.getCQLColumnName()).thenReturn("column");
        when(row.isNull("column")).thenReturn(true);
        when(meta.type()).thenReturn(PropertyType.SIMPLE);
        //When

        final Object decoded = extractor.invokeOnRowForFields(row);

        //Then
        assertThat(decoded).isNull();
    }

    @Test
    public void should_extract_compound_pk_from_row() throws Exception {
        //Given
        EntityMeta entityMeta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        CompleteBean pk = new CompleteBean();

        when(compoundPKProperties.getCQLComponentClasses()).thenReturn(Arrays.<Class<?>>asList(Long.class,String.class));
        when(compoundPKProperties.getCQLComponentNames()).thenReturn(asList("id", "name"));

        Definition columnDef1 = ColumnDefinitionBuilder.buildColumnDef("ks", "table", "id", DataType.bigint());
        Definition columnDef2 = ColumnDefinitionBuilder.buildColumnDef("ks", "table", "name", DataType.text());

        when(row.getColumnDefinitions().iterator()).thenReturn(Arrays.asList(columnDef1, columnDef2).iterator());
        when(row.getLong("id")).thenReturn(10L);
        when(row.getString("name")).thenReturn("DuyHai");

        when(entityMeta.structure().hasOnlyStaticColumns()).thenReturn(true);
        when(meta.forTranscoding().decodeFromComponents(anyList())).thenReturn(pk);

        //When
        final Object actual = extractor.extractCompoundPrimaryKeyFromRow(row, entityMeta, EntityState.MANAGED);

        //Then
        assertThat(actual).isSameAs(pk);
        verify(meta.getCompoundPKProperties()).getCQLComponentNames();
    }

    @Test
    public void should_extract_and_validate_compound_pk_from_row() throws Exception {
        //Given
        EntityMeta entityMeta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        CompleteBean pk = new CompleteBean();

        when(compoundPKProperties.getCQLComponentClasses()).thenReturn(Arrays.<Class<?>>asList(Long.class,String.class));
        when(compoundPKProperties.getCQLComponentNames()).thenReturn(asList("id", "name"));

        Definition columnDef1 = ColumnDefinitionBuilder.buildColumnDef("ks", "table", "id", DataType.bigint());
        Definition columnDef2 = ColumnDefinitionBuilder.buildColumnDef("ks", "table", "name", DataType.text());

        when(row.getColumnDefinitions().iterator()).thenReturn(Arrays.asList(columnDef1, columnDef2).iterator());
        when(row.getLong("id")).thenReturn(10L);
        when(row.getString("name")).thenReturn("DuyHai");

        when(entityMeta.structure().hasOnlyStaticColumns()).thenReturn(false);
        when(meta.forTranscoding().decodeFromComponents(anyList())).thenReturn(pk);

        //When
        final Object actual = extractor.extractCompoundPrimaryKeyFromRow(row, entityMeta, EntityState.MANAGED);

        //Then
        assertThat(actual).isSameAs(pk);
        verify(meta.getCompoundPKProperties(),times(2)).getCQLComponentNames();
    }
}