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


import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.junit.Test;

import com.datastax.driver.core.Cluster;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_DSE_5_0_0;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_DSE_5_0_0;
import info.archinnov.achilles.generated.manager.EntityWithDSESearchJSON_Manager;
import info.archinnov.achilles.internals.entities.EntityWithDSESearchJSON;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithDSESearchJSON {

    private static Cluster cluster = Cluster
            .builder()
            .addContactPoint("127.0.0.1")
            .build();

    private static ManagerFactory_For_IT_DSE_5_0_0 managerFactory =
            ManagerFactoryBuilder_For_IT_DSE_5_0_0
                    .builder(cluster)
            .doForceSchemaCreation(false)
            .withDefaultKeyspaceName("achilles_dse_it")
            .build();


    private static EntityWithDSESearchJSON_Manager manager = managerFactory.forEntityWithDSESearchJSON();
    private static ScriptExecutor scriptExecutor = new ScriptExecutor(manager.getNativeSession());
    private static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS'Z'";

    static {
        scriptExecutor.executeScript("EntityWithDSESearch/insertRows.cql");
    }


    @Test
    public void should_search_text_eq_JSON() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearchJSON> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_string().Eq_FromJson("\"speedrun\"")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.stream().map(EntityWithDSESearchJSON::getString).collect(toList()))
                .contains("speedrun");
    }

    @Test
    public void should_search_numeric_eq_JSON() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearchJSON> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_numeric().Eq_FromJson("100.03")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getNumeric()).isEqualTo(100.03f);
    }

    @Test
    public void should_search_date_eq_JSON() throws Exception {
        //Given
        final String searchedDate = "\"2016-09-26 08:00:00.000+0000\"";

        //When
        final List<EntityWithDSESearchJSON> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Eq_FromJson(searchedDate)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(toString(actual.get(0).getDate())).isEqualTo("2016-09-26 08:00:00.000Z");
    }

    @Test
    public void should_search_using_solr_and_partition_JSON() throws Exception {
        //Given
        final String searchedDate = "\"2016-09-25 13:00:00.000+0000\"";

        //When
        final List<EntityWithDSESearchJSON> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Eq_FromJson(searchedDate)
                .id().Eq_FromJson("3")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getNumeric()).isEqualTo(87.39f);
    }

    private String toString(Date date) {
        final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(date);
    }

}
