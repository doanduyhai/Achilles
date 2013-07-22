package info.archinnov.achilles.query;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.validation.Validator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SliceQueryValidator
 * 
 * @author DuyHai DOAN
 * 
 */

public class SliceQueryValidator {
    private static final Logger log = LoggerFactory.getLogger(SliceQueryValidator.class);

    public void validateClusteringKeys(PropertyMeta<?, ?> propertyMeta, List<Comparable<?>> startClustering,
            List<Comparable<?>> endClustering) {

        String startDescription = StringUtils.join(startClustering, ",");
        String endDescription = StringUtils.join(endClustering, ",");

        log.trace("Check compound keys {} / {}", startDescription, endDescription);

        int startIndex = findLastNonNullIndexForComponents(startClustering);
        int endIndex = findLastNonNullIndexForComponents(endClustering);

        // No more than 1 non-null component difference between clustering keys
        Validator.validateTrue(Math.abs(endIndex - startIndex) <= 1,
                "There should be no more than 1 component difference between clustering keys: [[" + startDescription
                        + "],[" + endDescription + "]");

        if (startIndex < 0 || endIndex < 0) {
            return;
        } else {
            int equalComponentsIndex = Math.max(startIndex, endIndex) - 1;
            for (int i = 0; i <= equalComponentsIndex; i++) {
                Comparable startComp = startClustering.get(i);
                Comparable endComp = endClustering.get(i);
                Validator.validateTrue(startComp.getClass() == endComp.getClass(), (i + 1)
                        + "th component for clustering keys should be of same type: [[" + startDescription + "],["
                        + endDescription + "]");
                Validator.validateTrue(startComp.compareTo(endComp) == 0, (i + 1)
                        + "th component for clustering keys should be equal: [[" + startDescription + "],["
                        + endDescription + "]");
            }

            if (startIndex == endIndex) {
                Comparable startComp = startClustering.get(startIndex);
                Comparable endComp = endClustering.get(endIndex);
                Validator.validateTrue(startComp.getClass() == endComp.getClass(), (startIndex + 1)
                        + "th component for clustering keys should be of same type: [[" + startDescription + "],["
                        + endDescription + "]");
                Validator.validateTrue(startComp.compareTo(endComp) < 0,
                        "Start clustering last component should be strictly 'less' than end clustering last component: [["
                                + startDescription + "],[" + endDescription + "]");
            }
        }

    }

    public int findLastNonNullIndexForComponents(List<Comparable<?>> components) {
        String description = StringUtils.join(components, ",");
        boolean nullFlag = false;
        int lastNotNullIndex = 0;
        if (components == null) {
            return -1;
        }

        for (Object component : components) {

            if (component == null) {
                nullFlag = true;
                continue;
            } else {
                if (nullFlag) {
                    throw new IllegalArgumentException(
                            "There should not be any null value between two non-null components for clustering keys '"
                                    + description + "'");
                }
                lastNotNullIndex++;
            }
        }
        lastNotNullIndex--;

        log.trace("Last non null index for components of property {} : {}", description, lastNotNullIndex);
        return lastNotNullIndex;
    }
}
