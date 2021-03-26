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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.junit.Test;

import com.datastax.driver.core.Cluster;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_DSE_4_8;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_DSE_4_8;
import info.archinnov.achilles.generated.manager.EntityWithDSESearch_Manager;
import info.archinnov.achilles.internals.entities.EntityWithDSESearch;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithDSESearch {

    private static Cluster cluster = Cluster
            .builder()
            .addContactPoint("127.0.0.1")
            .build();

    private static ManagerFactory_For_IT_DSE_4_8 managerFactory =
            ManagerFactoryBuilder_For_IT_DSE_4_8
                    .builder(cluster)
            .doForceSchemaCreation(false)
            .withDefaultKeyspaceName("achilles_dse_it")
            .build();


    private static EntityWithDSESearch_Manager manager = managerFactory.forEntityWithDSESearch();
    private static ScriptExecutor scriptExecutor = new ScriptExecutor(manager.getNativeSession());
    private static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS'Z'";
    static {
        scriptExecutor.executeScript("EntityWithDSESearch/insertRows.cql");
    }


    @Test
    public void should_search_text_using_prefix() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_string().StartWith("speed")
                .getList();

        //Then
        assertThat(actual).hasSize(2);
        assertThat(actual.stream().map(EntityWithDSESearch::getString).collect(toList()))
                .contains("speedster", "speedrun");
    }

    @Test
    public void should_search_text_using_suffix() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_string().EndWith("run")
                .getList();

        //Then
        assertThat(actual).hasSize(2);
        assertThat(actual.stream().map(EntityWithDSESearch::getString).collect(toList()))
                .contains("long run", "speedrun");
    }

    @Test
    public void should_search_text_using_substring() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_string().Contains("eds")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.stream().map(EntityWithDSESearch::getString).collect(toList()))
                .contains("speedster");
    }

    @Test
    public void should_search_numeric_eq() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_numeric().Eq(100.03f)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getNumeric()).isEqualTo(100.03f);
    }

    @Test
    public void should_search_numeric_gt() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_numeric().Gt(100.03f)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getNumeric()).isEqualTo(138.47f);
    }

    @Test
    public void should_search_numeric_gte() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_numeric().Gte(138.47f)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getNumeric()).isEqualTo(138.47f);
    }

    @Test
    public void should_search_numeric_lt() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_numeric().Lt(100.03f)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getNumeric()).isEqualTo(87.39f);
    }

    @Test
    public void should_search_numeric_lte() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_numeric().Lte(87.39f)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getNumeric()).isEqualTo(87.39f);
    }

    @Test
    public void should_search_numeric_gt_and_lt() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_numeric().Gt_And_Lt(87.39f, 138.47f)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getNumeric()).isEqualTo(100.03f);
    }

    @Test
    public void should_search_numeric_gt_and_lte() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_numeric().Gt_And_Lte(87.39f, 138.47f)
                .getList();

        //Then
        assertThat(actual).hasSize(2);
        assertThat(actual.stream().map(EntityWithDSESearch::getNumeric).collect(toList()))
                .contains(100.03f, 138.47f);
    }

    @Test
    public void should_search_numeric_gte_and_lt() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_numeric().Gte_And_Lt(87.39f, 138.47f)
                .getList();

        //Then
        assertThat(actual).hasSize(2);
        assertThat(actual.stream().map(EntityWithDSESearch::getNumeric).collect(toList()))
                .contains(87.39f, 100.03f);
    }

    @Test
    public void should_search_numeric_gte_and_lte() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_numeric().Gte_And_Lte(87.39f, 138.47f)
                .getList();

        //Then
        assertThat(actual).hasSize(3);
        assertThat(actual.stream().map(EntityWithDSESearch::getNumeric).collect(toList()))
                .contains(87.39f, 100.03f, 138.47f);
    }


    @Test
    public void should_search_date_eq() throws Exception {
        //Given
        final Date searchedDate = toDate("2016-09-26 08:00:00.000Z");

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Eq(searchedDate)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(toString(actual.get(0).getDate())).isEqualTo("2016-09-26 08:00:00.000Z");
    }

    @Test
    public void should_search_date_gt() throws Exception {
        //Given
        final Date searchedDate = toDate("2016-09-26 08:00:00.000Z");

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Gt(searchedDate)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(toString(actual.get(0).getDate())).isEqualTo("2016-09-26 09:00:00.000Z");
    }

    @Test
    public void should_search_date_gte() throws Exception {
        //Given
        final Date searchedDate = toDate("2016-09-26 08:00:00.000Z");

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Gte(searchedDate)
                .getList();

        //Then
        assertThat(actual).hasSize(2);
        assertThat(actual.stream()
                .map(EntityWithDSESearch::getDate)
                .map(this::toString)
                .collect(toList()))
            .contains("2016-09-26 08:00:00.000Z", "2016-09-26 09:00:00.000Z");
    }

    @Test
    public void should_search_date_lt() throws Exception {
        //Given
        final Date searchedDate = toDate("2016-09-26 08:00:00.000Z");

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Lt(searchedDate)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(toString(actual.get(0).getDate())).isEqualTo("2016-09-25 13:00:00.000Z");
    }

    @Test
    public void should_search_date_lte() throws Exception {
        //Given
        final Date searchedDate = toDate("2016-09-26 08:00:00.000Z");

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Lte(searchedDate)
                .getList();

        //Then
        assertThat(actual).hasSize(2);
        assertThat(actual.stream()
                .map(EntityWithDSESearch::getDate)
                .map(this::toString)
                .collect(toList()))
                .contains("2016-09-26 08:00:00.000Z", "2016-09-25 13:00:00.000Z");
    }


    @Test
    public void should_search_date_gt_and_lt() throws Exception {
        //Given
        final Date searchedDate1 = toDate("2016-09-25 13:00:00.000Z");
        final Date searchedDate2 = toDate("2016-09-26 09:00:00.000Z");

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Gt_And_Lt(searchedDate1, searchedDate2)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.stream()
                .map(EntityWithDSESearch::getDate)
                .map(this::toString)
                .collect(toList()))
                .contains("2016-09-26 08:00:00.000Z");
    }

    @Test
    public void should_search_date_gt_and_lte() throws Exception {
        //Given
        final Date searchedDate1 = toDate("2016-09-25 13:00:00.000Z");
        final Date searchedDate2 = toDate("2016-09-26 09:00:00.000Z");

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Gt_And_Lte(searchedDate1, searchedDate2)
                .getList();

        //Then
        assertThat(actual).hasSize(2);
        assertThat(actual.stream()
                .map(EntityWithDSESearch::getDate)
                .map(this::toString)
                .collect(toList()))
                .contains("2016-09-26 08:00:00.000Z", "2016-09-26 09:00:00.000Z");
    }

    @Test
    public void should_search_date_gte_and_lt() throws Exception {
        //Given
        final Date searchedDate1 = toDate("2016-09-25 13:00:00.000Z");
        final Date searchedDate2 = toDate("2016-09-26 09:00:00.000Z");

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Gte_And_Lt(searchedDate1, searchedDate2)
                .getList();

        //Then
        assertThat(actual).hasSize(2);
        assertThat(actual.stream()
                .map(EntityWithDSESearch::getDate)
                .map(this::toString)
                .collect(toList()))
                .contains("2016-09-26 08:00:00.000Z", "2016-09-25 13:00:00.000Z");
    }

    @Test
    public void should_search_date_gte_and_lte() throws Exception {
        //Given
        final Date searchedDate1 = toDate("2016-09-25 13:00:00.000Z");
        final Date searchedDate2 = toDate("2016-09-26 09:00:00.000Z");

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Gte_And_Lte(searchedDate1, searchedDate2)
                .getList();

        //Then
        assertThat(actual).hasSize(3);
        assertThat(actual.stream()
                .map(EntityWithDSESearch::getDate)
                .map(this::toString)
                .collect(toList()))
                .contains("2016-09-26 09:00:00.000Z",
                        "2016-09-26 08:00:00.000Z",
                        "2016-09-25 13:00:00.000Z");
    }

    @Test
    public void should_search_using_multiple_predicates() throws Exception {
        //Given
        final Date searchedDate1 = toDate("2016-09-25 13:00:00.000Z");
        final Date searchedDate2 = toDate("2016-09-26 09:00:00.000Z");

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Gte_And_Lte(searchedDate1, searchedDate2)
                .search_on_string().Contains("run")
                .search_on_numeric().Gt_And_Lte(80f, 110f)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getNumeric()).isEqualTo(87.39f);
    }

    @Test
    public void should_search_using_raw_predicates() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_string().RawPredicate("*eed?u*")
                .search_on_numeric().RawPredicate("{100 TO 150}")
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getString()).isEqualTo("speedrun");
    }

    @Test
    public void should_search_using_raw_solr_query() throws Exception {
        //Given

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .rawSolrQuery("string:*eed?u* OR numeric:[100 TO *]")
                .getList();

        //Then
        assertThat(actual).hasSize(2);
        assertThat(actual.stream()
                .map(EntityWithDSESearch::getString)
                .collect(toList()))
                .contains("speedrun","speedster");
    }

    @Test
    public void should_search_using_solr_and_partition() throws Exception {
        //Given
        final Date searchedDate1 = toDate("2016-09-25 13:00:00.000Z");
        final Date searchedDate2 = toDate("2016-09-26 09:00:00.000Z");

        //When
        final List<EntityWithDSESearch> actual = manager
                .indexed()
                .select()
                .allColumns_FromBaseTable()
                .where()
                .search_on_date().Gte_And_Lte(searchedDate1, searchedDate2)
                .search_on_string().Contains("run")
                .id().Eq(3L)
                .getList();

        //Then
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getNumeric()).isEqualTo(87.39f);
    }

    private Date toDate(String dateAsString) throws ParseException {
        return new SimpleDateFormat(DATE_FORMAT).parse(dateAsString);
    }

    private String toString(Date date) {
        final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(date);
    }

}
