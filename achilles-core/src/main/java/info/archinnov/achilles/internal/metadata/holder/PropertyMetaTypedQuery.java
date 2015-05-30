package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.internal.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PropertyMetaTypedQuery extends PropertyMetaView {

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaTypedQuery.class);

    protected PropertyMetaTypedQuery(PropertyMeta meta) {
        super(meta);
    }

    private static final Pattern SELECT_COLUMNS_PATTERN = Pattern.compile("select (.+) from .+");

    public void validateTypedQuery(String queryString, List<String> staticColumns) {
        log.trace("Validate typed query string {} for entity class {}", queryString, meta.getEntityClassName());

        boolean hasStaticColumns = false;
        final Matcher matcher = SELECT_COLUMNS_PATTERN.matcher(queryString);
        if (matcher.matches()) {
            for (String column : matcher.group(1).split(",")) {
                if (staticColumns.contains(column.trim())) {
                    hasStaticColumns = true;
                    break;
                }
            }
        }

        if (meta.structure().isCompoundPK()) {
            if (hasStaticColumns) {
                for (String component : meta.getCompoundPKProperties().getPartitionComponents().getCQLComponentNames()) {
                    Validator.validateTrue(queryString.contains(component),
                            "The typed query [%s] should contain the partition key component '%s' for compound primary key type '%s'",
                            queryString, component, meta.getValueClass().getCanonicalName());
                }
            } else {
                for (String component : meta.getCompoundPKProperties().getCQLComponentNames()) {
                    Validator.validateTrue(queryString.contains(component),
                            "The typed query [%s] should contain the component column '%s' for compound primary key type '%s'",
                            queryString, component, meta.getValueClass().getCanonicalName());
                }
            }
        } else {
            Validator.validateTrue(queryString.contains(meta.getCQLColumnName()), "The typed query [%s] should contain the id column '%s'", queryString, meta.getCQLColumnName());
        }
    }

}
