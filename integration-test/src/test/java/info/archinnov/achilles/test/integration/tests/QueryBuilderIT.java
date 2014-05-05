/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
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
package info.archinnov.achilles.test.integration.tests;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.query.typed.TypedQuery;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.Tweet;

import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static org.fest.assertions.api.Assertions.assertThat;

public class QueryBuilderIT {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private PersistenceManagerFactory pmf = CassandraEmbeddedServerBuilder
			.withEntityPackages(CompleteBean.class.getPackage().getName()).withKeyspaceName("query_builder_keyspace1")
			.buildPersistenceManagerFactory();

	private PersistenceManager manager = pmf.createPersistenceManager();

    @Test
    public void should_apply_null_heap_byte_buffer(){
        // Given
        Tweet entity = TweetTestBuilder.tweet().randomId().content("label").buid();

        manager.persist(entity);

        final Select.Where select = QueryBuilder.select().from("Tweet").where(QueryBuilder.eq("id", entity.getId()));
        final TypedQuery<Tweet> queryBuilder = manager.typedQuery(Tweet.class, select.getQueryString(), select.getValues());

        // When
        final Tweet actual = queryBuilder.getFirst();

        // Then
        assertThat(actual).isNotNull();
    }

    @Test
    public void should_apply_null_bounded_values(){
        // Given
        CompleteBean entity = builder().randomId().name("DuyHai").label("label").buid();

        manager.persist(entity);

        final Select.Where select = QueryBuilder.select().from("CompleteBean").where(QueryBuilder.eq("id", entity.getId()));
        final TypedQuery<CompleteBean> queryBuilder = manager.typedQuery(CompleteBean.class, select.getQueryString(), select.getValues());

        // When
        final CompleteBean actual = queryBuilder.getFirst();

        // Then
        assertThat(actual.getLabel()).isEqualTo("label");
    }
}
