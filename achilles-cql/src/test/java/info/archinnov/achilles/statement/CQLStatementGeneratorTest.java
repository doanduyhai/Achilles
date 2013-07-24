package info.archinnov.achilles.statement;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * CQLStringStatementGeneratorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLStatementGeneratorTest {

    @InjectMocks
    private CQLStatementGenerator generator;

    @Mock
    private SliceQueryStatementGenerator sliceQueryGenerator;

    @Mock
    private CQLSliceQuery<ClusteredEntity> sliceQuery;

    @Captor
    private ArgumentCaptor<Select> selectCaptor;

    @Test
    public void should_create_select_statement_for_entity_simple_id() throws Exception {
        EntityMeta meta = prepareEntityMeta("id");

        Select select = generator.generateSelectEntity(meta);

        assertThat(select.getQueryString()).isEqualTo("SELECT id,age,name,label FROM table;");
    }

    @Test
    public void should_create_select_statement_for_entity_compound_id() throws Exception {

        EntityMeta meta = prepareEntityMeta("id", "a", "b");

        Select select = generator.generateSelectEntity(meta);

        assertThat(select.getQueryString()).isEqualTo("SELECT id,a,b,age,name,label FROM table;");
    }

    @Test
    public void should_generate_slice_query() throws Exception
    {
        EntityMeta meta = prepareEntityMeta("id", "comp1", "comp2");
        when(sliceQuery.getMeta()).thenReturn(meta);
        when(sliceQuery.getLimit()).thenReturn(99);
        when(sliceQuery.getOrdering()).thenReturn(QueryBuilder.desc("comp1"));
        when(sliceQuery.getConsistencyLevel()).thenReturn(ConsistencyLevel.EACH_QUORUM);
        when(sliceQueryGenerator.generateWhereClauseForSliceQuery(eq(sliceQuery), any(Select.class)))
                .thenAnswer(new Answer<Statement>() {

                    @Override
                    public Statement answer(InvocationOnMock invocation) throws Throwable {
                        return buildFakeWhere((Select) invocation.getArguments()[1]);
                    }
                });

        Query query = generator.generateSliceQuery(sliceQuery);

        assertThat(query.toString()).isEqualTo(
                "SELECT id,comp1,comp2,age,name,label FROM table WHERE fake='fake' ORDER BY comp1 DESC LIMIT 99;");
    }

    private EntityMeta prepareEntityMeta(String... componentNames) throws Exception
    {
        PropertyMeta<?, ?> idMeta;
        if (componentNames.length > 1)
        {
            idMeta = PropertyMetaTestBuilder
                    .completeBean(Void.class, CompoundKey.class)
                    .field("id")
                    .compNames(componentNames)
                    .type(PropertyType.EMBEDDED_ID)
                    .build();
        }
        else
        {
            idMeta = PropertyMetaTestBuilder
                    .completeBean(Void.class, Long.class)
                    .field(componentNames[0])
                    .type(ID)
                    .build();
        }

        PropertyMeta<?, ?> ageMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, Long.class)
                .field("age")
                .type(SIMPLE).build();

        PropertyMeta<?, ?> nameMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .type(SIMPLE).build();

        PropertyMeta<?, ?> labelMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("label")
                .type(SIMPLE)
                .build();

        EntityMeta meta = new EntityMeta();
        meta.setTableName("table");
        meta.setEagerMetas(Arrays.<PropertyMeta<?, ?>> asList(idMeta, ageMeta, nameMeta, labelMeta));
        meta.setIdMeta(idMeta);

        return meta;
    }

    private Statement buildFakeWhere(Select select)
    {
        return select.where().and(QueryBuilder.eq("fake", "fake"));
    }
}
