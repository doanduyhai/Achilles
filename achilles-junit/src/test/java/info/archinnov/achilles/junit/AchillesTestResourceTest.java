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

package info.archinnov.achilles.junit;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.internals.runtime.AbstractManagerFactory;

public class AchillesTestResourceTest {

    private String randomKeyspace = RandomStringUtils.randomAlphabetic(20).toLowerCase();

    @Rule
    public AchillesTestResource<AbstractManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .createAndUseKeyspace(randomKeyspace)
            .build((cluster, statementsCache) -> (AbstractManagerFactory) null);

    private Session session = resource.getNativeSession();

    @Test
    public void should_start_cassandra_and_create_session() throws Exception {
        //Given
        //When

        //Then
        assertThat(session).isNotNull();
        assertThat(session.getLoggedKeyspace()).isEqualTo(randomKeyspace);
        final Row one = session.execute("SELECT * FROM system.local LIMIT 1").one();
        assertThat(one).isNotNull();

    }
}