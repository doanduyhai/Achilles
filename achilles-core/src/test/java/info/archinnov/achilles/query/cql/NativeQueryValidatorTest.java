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

package info.archinnov.achilles.query.cql;

import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static org.fest.assertions.api.Assertions.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;

@RunWith(MockitoJUnitRunner.class)
public class NativeQueryValidatorTest {

    private NativeQueryValidator validator = new NativeQueryValidator();

    final Select statement1 = select().from("test");
    final Select.Where statement2 = select().from("test").where(eq("id", 10));
    final SimpleStatement statement3 = new SimpleStatement("Select * from table");

    final Insert statement4 = QueryBuilder.insertInto("test").value("id", 10);
    final Insert.Options statement5 = QueryBuilder.insertInto("test").value("id", 10).using(ttl(10));
    final SimpleStatement statement6 = new SimpleStatement("insert into test(id) values(10)");

    final Update.Where statement7 = update("test").with(set("col", 2)).where(eq("id", 10));
    final Update.Options statement8 = update("test").with(set("col", 2)).where(eq("id", 10)).using(ttl(100));
    final SimpleStatement statement9 = new SimpleStatement("Update test set col = 2 where id = 10");

    final Delete.Where statement10 = delete().all().from("test").where(eq("id", 10));
    final Delete.Options statement11 = delete().all().from("test").where(eq("id", 10)).using(timestamp(10L));
    final SimpleStatement statement12 = new SimpleStatement("delete from test where id = 10");

    @Test
    public void should_detect_select_statement() throws Exception {
        assertThat(validator.isSelectStatement(statement1)).isTrue();
        assertThat(validator.isSelectStatement(statement2)).isTrue();
        assertThat(validator.isSelectStatement(statement3)).isTrue();

        assertThat(validator.isSelectStatement(statement4)).isFalse();
        assertThat(validator.isSelectStatement(statement5)).isFalse();
        assertThat(validator.isSelectStatement(statement6)).isFalse();

        assertThat(validator.isSelectStatement(statement7)).isFalse();
        assertThat(validator.isSelectStatement(statement8)).isFalse();
        assertThat(validator.isSelectStatement(statement9)).isFalse();

        assertThat(validator.isSelectStatement(statement10)).isFalse();
        assertThat(validator.isSelectStatement(statement11)).isFalse();
        assertThat(validator.isSelectStatement(statement12)).isFalse();

    }

    @Test
    public void should_detect_insert_statement() throws Exception {
        assertThat(validator.isInsertStatement(statement1)).isFalse();
        assertThat(validator.isInsertStatement(statement2)).isFalse();
        assertThat(validator.isInsertStatement(statement3)).isFalse();

        assertThat(validator.isInsertStatement(statement4)).isTrue();
        assertThat(validator.isInsertStatement(statement5)).isTrue();
        assertThat(validator.isInsertStatement(statement6)).isTrue();

        assertThat(validator.isInsertStatement(statement7)).isFalse();
        assertThat(validator.isInsertStatement(statement8)).isFalse();
        assertThat(validator.isInsertStatement(statement9)).isFalse();

        assertThat(validator.isInsertStatement(statement10)).isFalse();
        assertThat(validator.isInsertStatement(statement11)).isFalse();
        assertThat(validator.isInsertStatement(statement12)).isFalse();
    }


    @Test
    public void should_detect_update_statement() throws Exception {
        assertThat(validator.isUpdateStatement(statement1)).isFalse();
        assertThat(validator.isUpdateStatement(statement2)).isFalse();
        assertThat(validator.isUpdateStatement(statement3)).isFalse();

        assertThat(validator.isUpdateStatement(statement4)).isFalse();
        assertThat(validator.isUpdateStatement(statement5)).isFalse();
        assertThat(validator.isUpdateStatement(statement6)).isFalse();

        assertThat(validator.isUpdateStatement(statement7)).isTrue();
        assertThat(validator.isUpdateStatement(statement8)).isTrue();
        assertThat(validator.isUpdateStatement(statement9)).isTrue();

        assertThat(validator.isUpdateStatement(statement10)).isFalse();
        assertThat(validator.isUpdateStatement(statement11)).isFalse();
        assertThat(validator.isUpdateStatement(statement12)).isFalse();
    }

    @Test
    public void should_detect_delete_statement() throws Exception {
        assertThat(validator.isDeleteStatement(statement1)).isFalse();
        assertThat(validator.isDeleteStatement(statement2)).isFalse();
        assertThat(validator.isDeleteStatement(statement3)).isFalse();

        assertThat(validator.isDeleteStatement(statement4)).isFalse();
        assertThat(validator.isDeleteStatement(statement5)).isFalse();
        assertThat(validator.isDeleteStatement(statement6)).isFalse();

        assertThat(validator.isDeleteStatement(statement7)).isFalse();
        assertThat(validator.isDeleteStatement(statement8)).isFalse();
        assertThat(validator.isDeleteStatement(statement9)).isFalse();

        assertThat(validator.isDeleteStatement(statement10)).isTrue();
        assertThat(validator.isDeleteStatement(statement11)).isTrue();
        assertThat(validator.isDeleteStatement(statement12)).isTrue();
    }
}
