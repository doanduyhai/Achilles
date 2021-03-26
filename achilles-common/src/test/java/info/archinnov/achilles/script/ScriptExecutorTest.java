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

package info.archinnov.achilles.script;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

@RunWith(MockitoJUnitRunner.class)
public class ScriptExecutorTest {

    @InjectMocks
    private ScriptExecutor scriptExecutor;

    @Mock
    private Session session;

    @Captor
    private ArgumentCaptor<SimpleStatement> statementCaptor;

    @Test
    public void should_load_script_as_lines() throws Exception {
        //When
        final List<String> lines = scriptExecutor.loadScriptAsLines("testScript.cql");

        //Then
        assertThat(lines).contains("CREATE TABLE IF NOT EXISTS test(", atIndex(0));
        assertThat(lines).contains("id int PRIMARY KEY,", atIndex(1));
        assertThat(lines).contains("value text", atIndex(2));
        assertThat(lines).contains(");", atIndex(3));

        assertThat(lines).contains("INSERT INTO test(id,value) VALUES(1,'test');", atIndex(4));

        assertThat(lines).contains("DELETE FROM test WHERE id=1;", atIndex(5));
    }

    @Test
    public void should_build_statements_from_lines() throws Exception {
        //Given
        List<String> lines = new ArrayList<>();
        lines.add("CREATE TABLE IF NOT EXISTS test(");
        lines.add("id int PRIMARY KEY,");
        lines.add("value text");
        lines.add(");");
        lines.add("INSERT INTO test(id,value) VALUES(1,';^');");
        lines.add("DELETE FROM test WHERE id=1;");

        //When
        final List<SimpleStatement> statements = scriptExecutor.buildStatements(lines);

        //Then
        assertThat(statements.get(0).getQueryString()).isEqualTo("CREATE TABLE IF NOT EXISTS test( id int PRIMARY KEY, value text );");
        assertThat(statements.get(1).getQueryString()).isEqualTo("INSERT INTO test(id,value) VALUES(1,';^');");
        assertThat(statements.get(2).getQueryString()).isEqualTo("DELETE FROM test WHERE id=1;");
    }

    @Test
    public void should_build_batch_statements_from_lines() throws Exception {
        //Given
        List<String> lines = new ArrayList<>();
        lines.add("BEGIN UNLOGGED BATCH USING TIMESTAMP 123456789");
        lines.add(" INSERT INTO test(id,value) VALUES(1,'test');");
        lines.add(" DELETE FROM test WHERE id=1;");
        lines.add("APPLY BATCH;");
        lines.add("INSERT INTO test(id,value) VALUES(2,'test2');");

        //When
        final List<SimpleStatement> statements = scriptExecutor.buildStatements(lines);

        //Then
        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getQueryString()).isEqualTo(" BEGIN UNLOGGED BATCH USING TIMESTAMP 123456789  INSERT INTO test(id,value) VALUES(1,'test');  DELETE FROM test WHERE id=1; APPLY BATCH;");
        assertThat(statements.get(1).getQueryString()).isEqualTo("INSERT INTO test(id,value) VALUES(2,'test2');");
    }

    @Test
    public void should_build_create_function_statements_from_lines_syntax_1() throws Exception {
        //Given
        List<String> lines = new ArrayList<>();
        lines.add("CREATE FUNCTION udf.maxof(val1 int, val2 int)");
        lines.add(" RETURNS NULL ON NULL INPUT");
        lines.add(" RETURNS int");
        lines.add("LANGUAGE java");
        lines.add("AS $$");
        lines.add("return Math.max(val1, val2);");
        lines.add("$$;");

        //When
        final List<SimpleStatement> statements = scriptExecutor.buildStatements(lines);

        //Then
        assertThat(statements).hasSize(1);
        assertThat(statements.get(0).getQueryString()).isEqualTo("CREATE FUNCTION udf.maxof(val1 int, val2 int)  RETURNS NULL ON NULL INPUT  RETURNS int LANGUAGE java AS $$return Math.max(val1, val2);$$;");
    }

    @Test
    public void should_build_create_function_statements_from_lines_syntax_2() throws Exception {
        //Given
        List<String> lines = new ArrayList<>();
        lines.add("CREATE FUNCTION udf.maxof(val1 int, val2 int)");
        lines.add(" RETURNS NULL ON NULL INPUT");
        lines.add(" RETURNS int");
        lines.add("LANGUAGE java");
        lines.add("AS");
        lines.add("$$");
        lines.add("return Math.max(val1, val2);");
        lines.add("$$;");

        //When
        final List<SimpleStatement> statements = scriptExecutor.buildStatements(lines);

        //Then
        assertThat(statements).hasSize(1);
        assertThat(statements.get(0).getQueryString()).isEqualTo("CREATE FUNCTION udf.maxof(val1 int, val2 int)  RETURNS NULL ON NULL INPUT  RETURNS int LANGUAGE java AS $$return Math.max(val1, val2);$$;");
    }

    @Test
    public void should_build_create_function_statements_from_lines_syntax_3() throws Exception {
        //Given
        List<String> lines = new ArrayList<>();
        lines.add("CREATE FUNCTION udf.maxof(val1 int, val2 int)");
        lines.add(" RETURNS NULL ON NULL INPUT");
        lines.add(" RETURNS int");
        lines.add("LANGUAGE java");
        lines.add("AS $$");
        lines.add("return Math.max(val1, val2);");
        lines.add("$$");
        lines.add(";");
        lines.add("SELECT * FROM zeppelin.table;");

        //When
        final List<SimpleStatement> statements = scriptExecutor.buildStatements(lines);

        //Then
        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getQueryString()).isEqualTo("CREATE FUNCTION udf.maxof(val1 int, val2 int)  RETURNS NULL ON NULL INPUT  RETURNS int LANGUAGE java AS $$return Math.max(val1, val2);$$ ;");
        assertThat(statements.get(1).getQueryString()).isEqualTo("SELECT * FROM zeppelin.table;");
    }

    @Test
    public void should_build_create_function_statements_from_lines_syntax_4() throws Exception {
        //Given
        List<String> lines = new ArrayList<>();
        lines.add("CREATE FUNCTION udf.maxof(val1 int, val2 int)");
        lines.add(" RETURNS NULL ON NULL INPUT");
        lines.add(" RETURNS int");
        lines.add("LANGUAGE java");
        lines.add("AS");
        lines.add("$$");
        lines.add("return Math.max(val1, val2);");
        lines.add("$$");
        lines.add(";");
        lines.add("SELECT * FROM zeppelin.table;");

        //When
        final List<SimpleStatement> statements = scriptExecutor.buildStatements(lines);

        //Then
        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getQueryString()).isEqualTo("CREATE FUNCTION udf.maxof(val1 int, val2 int)  RETURNS NULL ON NULL INPUT  RETURNS int LANGUAGE java AS $$return Math.max(val1, val2);$$ ;");
        assertThat(statements.get(1).getQueryString()).isEqualTo("SELECT * FROM zeppelin.table;");
    }

    @Test
    public void should_execute_script() throws Exception {
        //When
        scriptExecutor.executeScript("testScript.cql");

        //Then
        verify(session,times(3)).execute(statementCaptor.capture());

        final List<SimpleStatement> statements = statementCaptor.getAllValues();
        assertThat(statements.get(0).getQueryString()).isEqualTo("CREATE TABLE IF NOT EXISTS test( id int PRIMARY KEY, value text );");
        assertThat(statements.get(1).getQueryString()).isEqualTo("INSERT INTO test(id,value) VALUES(1,'test');");
        assertThat(statements.get(2).getQueryString()).isEqualTo("DELETE FROM test WHERE id=1;");
    }
}