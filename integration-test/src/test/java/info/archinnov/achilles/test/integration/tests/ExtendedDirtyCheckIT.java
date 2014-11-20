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

import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.type.OptionsBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.data.MapEntry.entry;

public class ExtendedDirtyCheckIT {
    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(AchillesTestResource.Steps.AFTER_TEST, "CompleteBean");

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private PersistenceManager manager = resource.getPersistenceManager();

    private CompleteBean bean;

    @Before
    public void setUp() {
        bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L).addFriends("foo", "bar")
                .addFollowers("George", "Paul").addPreference(1, "FR").addPreference(2, "Paris")
                .addPreference(3, "75014").buid();

        bean = manager.insert(bean);
    }

    @Test
    public void should_add_elements_to_list_with_ttl() throws Exception {
        //Given
        bean.getFriends().add("qux");
        bean.getFriends().add("tux");

        //When
        manager.update(bean, OptionsBuilder.withTtl(1));
        Thread.sleep(1000);

        //Then
        manager.refresh(bean);

        assertThat(bean.getFriends()).containsExactly("foo","bar");
    }

    @Test
    public void should_add_elements_to_list_with_timestamp() throws Exception {
        //Given
        bean.getFriends().add("qux");
        bean.getFriends().add("tux");

        //When
        manager.update(bean, OptionsBuilder.withTimestamp((System.currentTimeMillis()-100000)*1000));

        //Then
        manager.refresh(bean);

        assertThat(bean.getFriends()).containsExactly("foo","bar");
    }

    @Test
    public void should_add_elements_to_set_with_ttl() throws Exception {
        //Given
        bean.getFollowers().add("Sylvain");
        bean.getFollowers().add("Jonathan");

        //When
        manager.update(bean, OptionsBuilder.withTtl(1));
        Thread.sleep(1000);

        //Then
        manager.refresh(bean);

        assertThat(bean.getFollowers()).containsOnly("George", "Paul");
    }

    @Test
    public void should_add_elements_to_set_with_timestamp() throws Exception {
        //Given
        bean.getFollowers().add("Sylvain");
        bean.getFollowers().add("Jonathan");

        //When
        manager.update(bean, OptionsBuilder.withTimestamp((System.currentTimeMillis()-100000)*1000));

        //Then
        manager.refresh(bean);

        assertThat(bean.getFollowers()).containsOnly("George", "Paul");
    }

    @Test
    public void should_add_elements_to_map_with_ttl() throws Exception {
        //Given
        bean.getPreferences().put(4, "Cassandra");
        bean.getPreferences().put(5, "CQL");

        //When
        manager.update(bean, OptionsBuilder.withTtl(1));
        Thread.sleep(1000);

        //Then
        manager.refresh(bean);

        assertThat(bean.getPreferences())
                .hasSize(3)
                .contains(entry(1, "FR"),entry(2,"Paris"),entry(3,"75014"));
    }

    @Test
    public void should_add_elements_to_map_with_timestamp() throws Exception {
        //Given
        bean.getPreferences().put(4, "Cassandra");
        bean.getPreferences().put(5, "CQL");

        //When
        manager.update(bean, OptionsBuilder.withTimestamp((System.currentTimeMillis()-100000)*1000));

        //Then
        manager.refresh(bean);

        assertThat(bean.getPreferences())
                .hasSize(3)
                .contains(entry(1, "FR"),entry(2,"Paris"),entry(3,"75014"));
    }
}
