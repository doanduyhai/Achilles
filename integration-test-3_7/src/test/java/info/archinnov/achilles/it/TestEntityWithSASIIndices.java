/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.it;


import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_3_7;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_3_7;
import info.archinnov.achilles.generated.manager.EntityWithSASIIndices_Manager;
import info.archinnov.achilles.internals.entities.EntityWithSASIIndices;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithSASIIndices {

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_3_7> resource = AchillesTestResourceBuilder
            .forJunit()
            .createAndUseKeyspace("it_3_7")
            .entityClassesToTruncate(EntityWithSASIIndices.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_3_7
                    .builder(cluster)
                    .withDefaultKeyspaceName("it_3_7")
                    .withManagedEntityClasses(EntityWithSASIIndices.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .build());

    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithSASIIndices_Manager manager = resource.getManagerFactory().forEntityWithSASIIndices();

    @Test
    public void should_search_using_prefix_non_tokenizer() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithSASIIndices/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithSASIIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_prefixNonTokenizer().StartWith("speed")
                .indexed_numeric().Gte(10)
                .indexed_numeric().Lte(15)
                .getList();

        //Then
        assertThat(actual).hasSize(2);
        assertThat(actual.get(0).getPrefixNonTokenizer()).isEqualTo("speed runner");
        assertThat(actual.get(1).getPrefixNonTokenizer()).isEqualTo("speedster");
    }

    @Test
    public void should_search_using_like_non_tokenizer() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithSASIIndices/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithSASIIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_prefixNonTokenizer().Like("speedster")
                .indexed_sparse().Gte(13)
                .indexed_sparse().Lte(15)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getPrefixNonTokenizer()).isEqualTo("speedster");
    }

    @Test
    public void should_search_using_eq_non_tokenizer() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithSASIIndices/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithSASIIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_prefixNonTokenizer().Eq("speedster")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getPrefixNonTokenizer()).isEqualTo("speedster");
    }

    @Test
    public void should_search_using_end_with() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithSASIIndices/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithSASIIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_containsStandardAnalyzer().EndWith("man")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getContainsStandardAnalyzer()).isEqualTo("the loving man");
    }

    @Test
    public void should_search_using_contains() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithSASIIndices/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithSASIIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_containsStandardAnalyzer().Contains("love")
                .getList();

        //Then
        assertThat(actual).hasSize(5);
        assertThat(actual.get(0).getContainsStandardAnalyzer()).isEqualTo("forever love");
        assertThat(actual.get(1).getContainsStandardAnalyzer()).isEqualTo("lovers");
        assertThat(actual.get(2).getContainsStandardAnalyzer()).isEqualTo("the white gloves");
        assertThat(actual.get(3).getContainsStandardAnalyzer()).isEqualTo("the loving man");
        assertThat(actual.get(4).getContainsStandardAnalyzer()).isEqualTo("no love");
    }

    @Test
    public void should_search_using_like() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithSASIIndices/insertRows.cql", ImmutableMap.of("id", id));

        //When
        final List<EntityWithSASIIndices> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .indexed_containsStandardAnalyzer().Like("chair dance")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getContainsStandardAnalyzer()).isEqualTo("the chair is dancing");
    }
}
