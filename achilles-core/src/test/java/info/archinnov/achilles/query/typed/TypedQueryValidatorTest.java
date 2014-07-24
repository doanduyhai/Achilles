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
import java.util.ArrayList;
import java.util.HashMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

public class TypedQueryValidatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private TypedQueryValidator validator = new TypedQueryValidator();

    @Test
    public void should_exception_when_wrong_table() throws Exception {
        EntityMeta meta = new EntityMeta();
        meta.setPropertyMetas(new HashMap<String, PropertyMeta>());
        meta.setTableName("table");

        final RegularStatement statement = select().from("test").where(eq("id",10L));

        exception.expect(AchillesException.class);
        exception.expectMessage("The typed query [SELECT * FROM test WHERE id=10;] should contain the ' from table' clause if type is '"
                + CompleteBean.class.getCanonicalName() + "'");

        validator.validateRawTypedQuery(CompleteBean.class, statement, meta);
    }

    @Test
    public void should_exception_when_missing_id_column() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
                .type(PropertyType.ID).build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        final RegularStatement statement = select("name","age").from("table").where(eq("col", 10L));

        exception.expect(AchillesException.class);
        exception.expectMessage("The typed query [select name,age from table where col=10;] should contain the id column 'id'");

        validator.validateTypedQuery(CompleteBean.class, statement, meta);
    }

    @Test
    public void should_exception_when_missing_component_column_for_embedded_id() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, EmbeddedKey.class).field("id")
                .type(PropertyType.EMBEDDED_ID).compNames("id", "name").build();

        EntityMeta meta = new EntityMeta();
        meta.setAllMetasExceptIdAndCounters(new ArrayList<PropertyMeta>());
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        final RegularStatement statement = select("id","age").from("table").where(eq("id", 10L));

        exception.expect(AchillesException.class);
        exception.expectMessage("The typed query [select id,age from table where id=10;] should contain the component column 'name' for embedded id type '"
                + EmbeddedKey.class.getCanonicalName() + "'");

        validator.validateTypedQuery(CompleteBean.class, statement, meta);
    }

    @Test
    public void should_skip_id_column_validation_when_select_star() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
                .type(PropertyType.ID).build();

        EntityMeta meta = new EntityMeta();
        meta.setAllMetasExceptIdAndCounters(new ArrayList<PropertyMeta>());
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        final RegularStatement statement = select().from("table").where(eq("id",10L));

        validator.validateTypedQuery(CompleteBean.class, statement, meta);
    }

    @Test
    public void should_skip_component_column_validation_when_select_star() throws Exception {
        PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, EmbeddedKey.class).field("id")
                .type(PropertyType.EMBEDDED_ID).compNames("id", "name").build();

        EntityMeta meta = new EntityMeta();
        meta.setAllMetasExceptIdAndCounters(new ArrayList<PropertyMeta>());
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        final RegularStatement statement = select().from("table").where(eq("id", 10L));

        validator.validateTypedQuery(CompleteBean.class, statement, meta);
    }

    @Test(expected = AchillesException.class)
    public void should_exception_when_not_SELECT_statement() throws Exception {
        //Given
        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        RegularStatement statement = QueryBuilder.insertInto("test");

        //When
        validator.validateRawTypedQuery(CompleteBean.class,statement,meta);

        //Then

    }
}
