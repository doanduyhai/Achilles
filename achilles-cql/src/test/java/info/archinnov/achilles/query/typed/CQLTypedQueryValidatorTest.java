package info.archinnov.achilles.query.typed;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.util.ArrayList;
import java.util.HashMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * CQLTypedQueryValidatorTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLTypedQueryValidatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private CQLTypedQueryValidator validator = new CQLTypedQueryValidator();

    @Test
    public void should_exception_when_wrong_table() throws Exception
    {
        EntityMeta meta = new EntityMeta();
        meta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());
        meta.setTableName("table");

        String queryString = "SELECT * from test";

        exception.expect(AchillesException.class);
        exception
                .expectMessage("The typed query [SELECT * from test] should contain the ' from table' clause if type is '"
                        + CompleteBean.class.getCanonicalName() + "'");

        validator.validateRawTypedQuery(CompleteBean.class, queryString, meta);
    }

    @Test
    public void should_exception_when_missing_id_column() throws Exception
    {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .type(PropertyType.ID)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setAllMetasExceptIdMeta(new ArrayList<PropertyMeta<?, ?>>());
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        String queryString = "SELECT name,age from table";

        exception.expect(AchillesException.class);
        exception
                .expectMessage("The typed query [SELECT name,age from table] should contain the id column 'id'");

        validator.validateTypedQuery(CompleteBean.class, queryString, meta);
    }

    @Test
    public void should_exception_when_missing_component_column_for_embedded_id() throws Exception
    {
        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, CompoundKey.class)
                .field("id")
                .type(PropertyType.EMBEDDED_ID)
                .compNames("id", "name")
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setAllMetasExceptIdMeta(new ArrayList<PropertyMeta<?, ?>>());
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        String queryString = "SELECT id,age from table";

        exception.expect(AchillesException.class);
        exception
                .expectMessage("The typed query [SELECT id,age from table] should contain the component column 'name' for embedded id type '"
                        + CompoundKey.class.getCanonicalName() + "'");

        validator.validateTypedQuery(CompleteBean.class, queryString, meta);
    }

    @Test
    public void should_skip_id_column_validation_when_select_star() throws Exception
    {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("id")
                .type(PropertyType.ID)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setAllMetasExceptIdMeta(new ArrayList<PropertyMeta<?, ?>>());
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        String queryString = "SELECT * from table";

        validator.validateTypedQuery(CompleteBean.class, queryString, meta);
    }

    @Test
    public void should_skip_component_column_validation_when_select_star() throws Exception
    {
        PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, CompoundKey.class)
                .field("id")
                .type(PropertyType.EMBEDDED_ID)
                .compNames("id", "name")
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setAllMetasExceptIdMeta(new ArrayList<PropertyMeta<?, ?>>());
        meta.setTableName("table");
        meta.setIdMeta(idMeta);

        String queryString = "SELECT * from table";

        validator.validateTypedQuery(CompleteBean.class, queryString, meta);
    }
}
