package info.archinnov.achilles.query;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.statement.CQLStatementGenerator;
import info.archinnov.achilles.type.BoundingMode;
import java.lang.reflect.Method;
import java.util.List;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Lists;

/**
 * SliceQueryBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceQueryBuilder {
    private ReflectionInvoker invoker = new ReflectionInvoker();
    private SliceQueryValidator validator = new SliceQueryValidator();
    private CQLStatementGenerator generator = new CQLStatementGenerator();

    public Statement generateSelectStatement(EntityMeta meta, Object from, Object to, BoundingMode boundingMode) {
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();

        List<Method> componentGetters = idMeta.getComponentGetters();
        List<String> componentNames = idMeta.getComponentNames();

        List<Comparable<?>> startValues = Lists.newArrayList();
        List<Comparable<?>> endValues = Lists.newArrayList();
        //        if (from != null) {
        //            startValues = invoker.determineMultiKeyValues(from, componentGetters);
        //        }
        //        if (to != null) {
        //            endValues = invoker.determineMultiKeyValues(to, componentGetters);
        //        }

        Select select = generator.generateSelectEntity(meta);
        Statement statement = generator.generateWhereClauseForSliceQuery(componentNames, startValues, endValues,
                boundingMode, select);

        return statement;
    }
}
