/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.internal.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import info.archinnov.achilles.internal.table.SchemaReader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.Sets;

import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.ParsingResult;
import info.archinnov.achilles.internal.table.TableCreator;
import info.archinnov.achilles.internal.table.TableUpdater;
import info.archinnov.achilles.internal.table.TableValidator;

@RunWith(MockitoJUnitRunner.class)
public class SchemaContextTest {

    private SchemaContext context;

    @Mock
    private Session session;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Cluster cluster;

    @Mock
    private Map<Class<?>, EntityMeta> entityMetaMap;

    @Mock
    private TableCreator tableCreator;

	@Mock
	private TableUpdater tableUpdater;

    @Mock
    private TableValidator tableValidator;

    @Mock
    private SchemaReader schemaReader;

    @Mock
    private ConfigurationContext configContext;


    private String keyspaceName = "keyspace";

    @Before
    public void setUp() {
        when(configContext.isForceColumnFamilyCreation()).thenReturn(true);
        context = new SchemaContext(configContext, session, keyspaceName, cluster, new ParsingResult(entityMetaMap, true));
        context.tableCreator = tableCreator;
        context.tableUpdater = tableUpdater;
        context.tableValidator = tableValidator;
        context.schemaReader = schemaReader;
    }

    @Test
    public void should_get_session() throws Exception {
        // Then
        assertThat(context.getSession()).isSameAs(session);
    }

    @Test
    public void should_has_counter() throws Exception {
        // Then
        assertThat(context.hasSimpleCounter()).isTrue();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_return_entity_meta_entry_set() throws Exception {
        // Given
        Entry<Class<?>, EntityMeta> entry = mock(Entry.class);

        // When
        when(entityMetaMap.entrySet()).thenReturn(Sets.newHashSet(entry));

        Set<Entry<Class<?>, EntityMeta>> actual = context.entityMetaEntrySet();

        // Then
        assertThat(actual).containsOnly(entry);
    }

    @Test
    public void should_validate_for_entity() throws Exception {
        // Given
        EntityMeta entityMeta = mock(EntityMeta.class);
        TableMetadata tableMetaData = mock(TableMetadata.class);

        // When
        context.validateForEntity(entityMeta, tableMetaData);

        // Then
        verify(tableValidator).validateForEntity(entityMeta, tableMetaData, configContext);
    }

    @Test
    public void should_validate_achilles_counter() throws Exception {
        // Given
        KeyspaceMetadata keyspaceMeta = mock(KeyspaceMetadata.class);

        // When
        when(cluster.getMetadata().getKeyspace(keyspaceName)).thenReturn(keyspaceMeta);
        context.validateAchillesCounter();

        // Then
        verify(tableValidator).validateAchillesCounter(keyspaceMeta, keyspaceName);
    }

    @Test
    public void should_fetch_table_metas() throws Exception {
        // Given
        Map<String, TableMetadata> expected = new HashMap<>();
        EntityMeta meta = mock(EntityMeta.class, RETURNS_DEEP_STUBS);
        final List<EntityMeta> entityMetas = Arrays.asList(meta);
        when(entityMetaMap.values()).thenReturn(entityMetas);


        // When
        when(schemaReader.fetchTableMetaData(cluster, entityMetas)).thenReturn(expected);

        Map<String, TableMetadata> actual = context.fetchTableMetaData();

        // Then
        verify(entityMetaMap).values();
        assertThat(actual).isSameAs(expected);
    }

    @Test
    public void should_create_table_for_entity() throws Exception {
        // Given
        EntityMeta entityMeta = mock(EntityMeta.class);

        // When
        context.createTableForEntity(entityMeta);

        // Then
        verify(tableCreator).createTableForEntity(session, entityMeta, configContext);
    }

    @Test
	public void should_update_table_for_entity() throws Exception {
		// Given
		EntityMeta entityMeta = mock(EntityMeta.class);
		TableMetadata tableMetadata = mock(TableMetadata.class);

		// When
		context.updateForEntity(entityMeta, tableMetadata);

		// Then
		verify(tableUpdater).updateTableForEntity(session, entityMeta, tableMetadata);
	}

	@Test
	public void should_create_table_for_counter() throws Exception {

        // When
        context.createTableForCounter();

        // Then
        verify(tableCreator).createTableForCounter(session, configContext);
    }
}
