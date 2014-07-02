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

package info.archinnov.achilles.schemabuilder;

import static org.fest.assertions.api.Assertions.assertThat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import info.archinnov.achilles.schemabuilder.SchemaBuilder;

public class DropTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void should_drop_table() throws Exception {
        //When
        final String built = SchemaBuilder.dropTable("test").build();

        //Then
        assertThat(built).isEqualTo("DROP TABLE test");
    }

    @Test
    public void should_drop_table_with_keyspace() throws Exception {
        //When
        final String built = SchemaBuilder.dropTable("ks", "test").build();

        //Then
        assertThat(built).isEqualTo("DROP TABLE ks.test");
    }

    @Test
    public void should_drop_table_with_keyspace_if_exists() throws Exception {
        //When
        final String built = SchemaBuilder.dropTable("ks", "test").ifExists(true).build();

        //Then
        assertThat(built).isEqualTo("DROP TABLE IF EXISTS ks.test");
    }

    @Test
    public void should_fail_if_keyspace_name_is_a_reserved_keyword() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("The keyspace name 'add' is not allowed because it is a reserved keyword");

        SchemaBuilder.dropTable("add","test").build();
    }

    @Test
    public void should_fail_if_table_name_is_a_reserved_keyword() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("The table name 'add' is not allowed because it is a reserved keyword");

        SchemaBuilder.dropTable("add").build();
    }
}
