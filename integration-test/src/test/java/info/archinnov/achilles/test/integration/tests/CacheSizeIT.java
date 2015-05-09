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
package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.configuration.ConfigurationParameters.PREPARED_STATEMENTS_CACHE_SIZE;
import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.Arrays;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.entity.CompleteBean;

public class CacheSizeIT {

    private PersistenceManager pm = CassandraEmbeddedServerBuilder
            .withEntities(CompleteBean.class)
            .withKeyspaceName("prepared_statement_cache")
            .withAchillesConfigParams(ImmutableMap.<ConfigurationParameters, Object>of(PREPARED_STATEMENTS_CACHE_SIZE, 2))
            .cleanDataFilesAtStartup(true)
            .buildPersistenceManager();


    @Test
    public void should_re_prepare_statements_when_cache_size_exceeded() throws Exception {
        //Given
        CompleteBean bean = builder().id(RandomUtils.nextLong(0,Long.MAX_VALUE)).name("name").buid();

        pm.insert(bean);

        final CompleteBean proxy = pm.forUpdate(CompleteBean.class, bean.getId());

        //When
        proxy.setAge(10L);
        pm.update(proxy);

        proxy.setFriends(Arrays.asList("foo", "bar"));
        pm.update(proxy);

        proxy.setFollowers(Sets.newHashSet("George", "Paul"));
        pm.update(proxy);

        proxy.setAge(11L);
        pm.update(proxy);

        //Then
        CompleteBean found = pm.find(CompleteBean.class, bean.getId());

        assertThat(found.getAge()).isEqualTo(11L);
        assertThat(found.getName()).isEqualTo("name");
        assertThat(found.getFriends()).containsExactly("foo", "bar");
        assertThat(found.getFollowers()).containsOnly("George", "Paul");
    }
}
