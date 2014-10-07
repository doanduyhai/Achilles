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
package info.archinnov.achilles.query.typed;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static java.lang.String.format;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TypedQueryValidatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta idMeta;

    private TypedQueryValidator validator = new TypedQueryValidator();

    @Before
    public void setUp() {
        when(meta.getIdMeta()).thenReturn(idMeta);
        when(meta.config().getTableName()).thenReturn("table");
    }

    @Test
    public void should_validate_raw_typed_query() throws Exception {
        //Given
        final RegularStatement statement = select().from("ks", "table").where(eq("id", 10L));

        //When
        validator.validateRawTypedQuery(CompleteBean.class, statement, meta);

        //Then

    }

    @Test
    public void should_exception_when_wrong_table() throws Exception {
        final RegularStatement statement = select().from("ks", "test").where(eq("id", 10L));

        exception.expect(AchillesException.class);
        exception.expectMessage(format("The typed query [SELECT * FROM ks.test WHERE id=10;] should contain the table name 'table' if type is '%s'", CompleteBean.class.getCanonicalName()));

        validator.validateRawTypedQuery(CompleteBean.class, statement, meta);
    }

    @Test
    public void should_exception_when_missing_id_column() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(PropertyType.ID).build();

        when(meta.getIdMeta()).thenReturn(idMeta);

        final RegularStatement statement = select("name","age").from("table").where(eq("col", 10L));

        exception.expect(AchillesException.class);
        exception.expectMessage("The typed query [select name,age from table where col=10;] should contain the id column 'id'");

        validator.validateTypedQuery(CompleteBean.class, statement, meta);
    }

    @Test
    public void should_skip_id_column_validation_when_select_star() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(PropertyType.ID).build();

        when(meta.getIdMeta()).thenReturn(idMeta);

        final RegularStatement statement = select().from("table").where(eq("id",10L));

        validator.validateTypedQuery(CompleteBean.class, statement, meta);
    }


    @Test(expected = AchillesException.class)
    public void should_exception_when_not_SELECT_statement() throws Exception {
        //Given
        RegularStatement statement = QueryBuilder.insertInto("test");

        //When
        validator.validateRawTypedQuery(CompleteBean.class,statement,meta);

        //Then

    }
}
