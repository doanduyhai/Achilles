package info.archinnov.achilles.table;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.type.Counter;
import java.util.Arrays;
import org.junit.Test;

/**
 * CQLTableBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLTableBuilderTest {

    @Test
    public void should_build_table_with_all_types_and_comment() throws Exception
    {

        String ddlScript = CQLTableBuilder
                .createTable("tableName")
                .addColumn("longCol", Long.class)
                .addColumn("enumCol", PropertyType.class)
                .addColumn("objectCol", UserBean.class)
                .addList("listCol", Long.class)
                .addList("listEnumCol", PropertyType.class)
                .addList("listObjectCol", UserBean.class)
                .addSet("setCol", Long.class)
                .addSet("setEnumCol", PropertyType.class)
                .addSet("setObjectCol", UserBean.class)
                .addMap("mapCol", Integer.class, Long.class)
                .addMap("mapEnumKeyCol", PropertyType.class, Long.class)
                .addMap("mapEnumValCol", Integer.class, PropertyType.class)
                .addMap("mapObjectValCol", Integer.class, UserBean.class)
                .addPrimaryKeys(Arrays.asList("longCol", "enumCol"))
                .addComment("This is a comment for 'tableName'")
                .generateDDLScript();

        assertThat(ddlScript).isEqualTo("CREATE TABLE tableName(\n" +
                "\tlongCol bigint,\n" +
                "\tenumCol text,\n" +
                "\tobjectCol text,\n" +
                "\tlistObjectCol list<text>,\n" +
                "\tlistEnumCol list<text>,\n" +
                "\tlistCol list<bigint>,\n" +
                "\tsetCol set<bigint>,\n" +
                "\tsetEnumCol set<text>,\n" +
                "\tsetObjectCol set<text>,\n" +
                "\tmapEnumValCol map<int,text>,\n" +
                "\tmapEnumKeyCol map<text,bigint>,\n" +
                "\tmapCol map<int,bigint>,\n" +
                "\tmapObjectValCol map<int,text>,\n" +
                "\tPRIMARY KEY(longCol, enumCol)\n" +
                ") WITH COMMENT = 'This is a comment for \"tableName\"'");

    }

    @Test
    public void should_build_counter_table_with_primary_keys() throws Exception
    {

        String ddlScript = CQLTableBuilder
                .createCounterTable("tableName")
                .addColumn("longCol", Long.class)
                .addColumn("enumCol", PropertyType.class)
                .addColumn("counterColumn", Counter.class)
                .addPrimaryKeys(Arrays.asList("longCol", "enumCol"))
                .addComment("This is a comment for 'tableName'")
                .generateDDLScript();

        assertThat(ddlScript).isEqualTo("CREATE TABLE tableName(\n" +
                "\tlongCol bigint,\n" +
                "\tenumCol text,\n" +
                "\tcounterColumn counter,\n" +
                "\tPRIMARY KEY(longCol, enumCol)\n" +
                ") WITH COMMENT = 'This is a comment for \"tableName\"'");

    }
}
