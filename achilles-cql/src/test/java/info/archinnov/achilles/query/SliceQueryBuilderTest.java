package info.archinnov.achilles.query;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.statement.CQLStatementGenerator;
import info.archinnov.achilles.type.WideMap.BoundingMode;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import testBuilders.PropertyMetaTestBuilder;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;

/**
 * SliceQueryBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@Ignore
@RunWith(MockitoJUnitRunner.class)
public class SliceQueryBuilderTest {

    @InjectMocks
    private SliceQueryBuilder builder;

    @Mock
    private SliceQueryValidator validator;

    @Mock
    private MethodInvoker invoker;

    @Mock
    private CQLStatementGenerator generator;

    @Mock
    private Select select;

    @Mock
    private Statement statement;

    @Test
    public void should_generate_select_statement() throws Exception {
        List<String> compNames = Arrays.<String> asList();
        List<Method> compGetters = Arrays.<Method> asList();
        List<Object> startValues = Arrays.<Object> asList();
        List<Object> endValues = Arrays.<Object> asList();

        BoundingMode boundingMode = BoundingMode.INCLUSIVE_BOUNDS;

        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
                .compNames(compNames).compGetters(compGetters).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);

        when(invoker.determineMultiKeyValues(10L, compGetters)).thenReturn(startValues);
        when(invoker.determineMultiKeyValues(11L, compGetters)).thenReturn(endValues);

        when(generator.generateSelectEntity(meta)).thenReturn(select);
        //		when(
        //				generator.generateWhereClauseForSliceQuery(compNames, startValues, endValues,
        //						boundingMode, select)).thenReturn(statement);
        Statement actual = builder.generateSelectStatement(meta, 10L, 11L, boundingMode);

        assertThat(actual).isSameAs(statement);
    }

    @Test
    public void should_generate_select_statement_with_null_from() throws Exception {
        List<String> compNames = Arrays.<String> asList();
        List<Method> compGetters = Arrays.<Method> asList();
        List<Object> startValues = Arrays.<Object> asList();
        List<Object> endValues = Arrays.<Object> asList();

        BoundingMode boundingMode = BoundingMode.INCLUSIVE_BOUNDS;

        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
                .compNames(compNames).compGetters(compGetters).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);

        when(invoker.determineMultiKeyValues(11L, compGetters)).thenReturn(endValues);

        when(generator.generateSelectEntity(meta)).thenReturn(select);
        //        when(generator.generateWhereClauseForSliceQuery(compNames, startValues, endValues, boundingMode, select))
        //                .thenReturn(statement);
        Statement actual = builder.generateSelectStatement(meta, null, 11L, boundingMode);

        assertThat(actual).isSameAs(statement);
        verify(invoker, never()).determineMultiKeyValues(null, compGetters);
    }

    @Test
    public void should_generate_select_statement_with_null_to() throws Exception {
        List<String> compNames = Arrays.<String> asList();
        List<Method> compGetters = Arrays.<Method> asList();
        List<Object> startValues = Arrays.<Object> asList();
        List<Object> endValues = Arrays.<Object> asList();

        BoundingMode boundingMode = BoundingMode.INCLUSIVE_BOUNDS;

        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id")
                .compNames(compNames).compGetters(compGetters).build();

        EntityMeta meta = new EntityMeta();
        meta.setIdMeta(idMeta);

        when(invoker.determineMultiKeyValues(10L, compGetters)).thenReturn(endValues);

        when(generator.generateSelectEntity(meta)).thenReturn(select);
        //        when(generator.generateWhereClauseForSliceQuery(compNames, startValues, endValues, boundingMode, select))
        //                .thenReturn(statement);
        Statement actual = builder.generateSelectStatement(meta, 10L, null, boundingMode);

        assertThat(actual).isSameAs(statement);
        verify(invoker, never()).determineMultiKeyValues(null, compGetters);
    }
}
