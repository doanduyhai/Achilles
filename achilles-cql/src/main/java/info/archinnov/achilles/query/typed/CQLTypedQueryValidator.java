package info.archinnov.achilles.query.typed;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CQLTypedQueryValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLTypedQueryValidator {

    private static final Logger logger = LoggerFactory.getLogger(CQLTypedQueryValidator.class);

    public void validateTypedQuery(Class<?> entityClass, String queryString, EntityMeta meta) {
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        String normalizedQuery = queryString.toLowerCase();

        validateRawTypedQuery(entityClass, queryString, meta);

        if (!normalizedQuery.contains("select *"))
        {
            if (idMeta.isEmbeddedId())
            {

                for (String component : idMeta.getComponentNames())
                {
                    Validator.validateTrue(normalizedQuery.contains(component.toLowerCase()), "The typed query ["
                            + queryString + "] should contain the component column '" + component
                            + "' for embedded id type '" + idMeta.getValueClass().getCanonicalName() + "'");
                }
            }
            else
            {
                String idColumn = idMeta.getPropertyName();
                Validator.validateTrue(normalizedQuery.contains(idColumn.toLowerCase()), "The typed query ["
                        + queryString + "] should contain the id column '" + idColumn + "'");
            }
        }
    }

    public void validateRawTypedQuery(Class<?> entityClass, String queryString, EntityMeta meta) {
        String tableName = meta.getTableName().toLowerCase();
        String normalizedQuery = queryString.toLowerCase();

        Validator.validateTrue(
                normalizedQuery.contains(" from " + tableName),
                "The typed query [" + queryString + "] should contain the ' from " + tableName
                        + "' clause if type is '"
                        + entityClass.getCanonicalName() + "'");

        for (PropertyMeta<?, ?> pm : meta.getAllMetasExceptIdMeta())
        {
            String column = pm.getPropertyName().toLowerCase();
            if (pm.isJoin() && normalizedQuery.contains(column))
            {
                logger.warn(
                        "The column '{}' in the type query [{}] is a join column and will not be mapped to the entity '{}'",
                        column, queryString, entityClass.getCanonicalName());
            }
        }
    }
}
