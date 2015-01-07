package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.internal.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyMetaTypedQuery extends PropertyMetaView {

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaTypedQuery.class);

    protected PropertyMetaTypedQuery(PropertyMeta meta) {
        super(meta);
    }

    public void validateTypedQuery(String queryString) {
        log.trace("Validate typed query string {} for entity class {}", queryString, meta.getEntityClassName());
        if (meta.structure().isCompoundPK()) {
            for (String component : meta.getCompoundPKProperties().getCQLComponentNames()) {
                Validator.validateTrue(queryString.contains(component),
                        "The typed query [%s] should contain the component column '%s' for compound primary key type '%s'",
                        queryString, component, meta.getCqlValueClass().getCanonicalName());
            }
        } else {
            Validator.validateTrue(queryString.contains(meta.getCQLColumnName()), "The typed query [%s] should contain the id column '%s'", queryString, meta.getCQLColumnName());
        }
    }

}
