package info.archinnov.achilles.compound;

import static info.archinnov.achilles.type.OrderingMode.ASCENDING;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftCompoundKeyValidator
{
    private static final Logger log = LoggerFactory.getLogger(ThriftCompoundKeyValidator.class);

    private ThriftCompoundKeyMapper mapper = new ThriftCompoundKeyMapper();
    private ComponentComparator comparator = new ComponentComparator();

    public void validatePartitionKey(PropertyMeta<?, ?> pm, Object... partitionKeys)
    {
        String className = pm.getEntityClassName();
        Validator.validateNotNull(partitionKeys,
                "There should be at least one partition key provided for querying on entity '"
                        + className + "'");
        Validator.validateTrue(partitionKeys.length > 0,
                "There should be at least one partition key provided for querying on entity '"
                        + className + "'");
        Class<?> partitionKeyType = pm.getComponentClasses().get(0);

        for (Object partitionKey : partitionKeys)
        {
            Class<?> type = partitionKey.getClass();

            Validator.validateTrue(type.equals(partitionKeyType),
                    "The type '" + type.getCanonicalName()
                            + "' of partition key '" + partitionKey + "' for querying on entity '"
                            + className + "' is not valid. It should be '"
                            + partitionKeyType.getCanonicalName() + "'");
        }

    }

    public void validateClusteringKeys(PropertyMeta<?, ?> pm, Object... clusteringKeys)
    {
        String className = pm.getEntityClassName();
        Validator.validateNotNull(clusteringKeys,
                "There should be at least one clustering key provided for querying on entity '"
                        + className + "'");

        List<Class<?>> clusteringClasses = pm.getComponentClasses().subList(1,
                pm.getComponentClasses().size());
        int maxClusteringCount = clusteringClasses.size();

        Validator.validateTrue(clusteringKeys.length <= maxClusteringCount,
                "There should be at most "
                        + maxClusteringCount
                        + " value(s) of clustering component(s) provided for querying on entity '"
                        + className
                        + "'");

        mapper.validateNoHoleAndReturnLastNonNullIndex(Arrays.<Object> asList(clusteringKeys));

        for (int i = 0; i < clusteringKeys.length; i++)
        {
            Object clusteringKey = clusteringKeys[i];
            if (clusteringKey != null)
            {
                Class<?> clusteringType = clusteringKey.getClass();
                Class<?> expectedClusteringType = clusteringClasses.get(i);

                Validator.validateComparable(clusteringType,
                        "The type '" + clusteringType.getCanonicalName()
                                + "' of clustering key '" + clusteringKey
                                + "' for querying on entity '"
                                + className + "' should implement the Comparable<T> interface");

                Validator.validateTrue(expectedClusteringType.equals(clusteringType),
                        "The type '" + clusteringType.getCanonicalName()
                                + "' of clustering key '" + clusteringKey
                                + "' for querying on entity '"
                                + className + "' is not valid. It should be '"
                                + expectedClusteringType.getCanonicalName() + "'");
            }

        }
    }

    public void validateCompoundKeysForClusteredQuery(PropertyMeta<?, ?> propertyMeta, List<Object> start,
            List<Object> end, OrderingMode ordering)
    {
        Validator.validateNotNull(start.get(0),
                "Partition key should not be null for start clustering key : "
                        + start);
        Validator.validateNotNull(end.get(0),
                "Partition key should not be null for end clustering key : "
                        + end);
        Validator.validateTrue(
                start.get(0).equals(end.get(0)),
                "Partition key should be equal for start and end clustering keys : ["
                        + start + "," + end + "]");

        validateComponentsForQuery(start.subList(0, start.size()),
                end.subList(0, end.size()), ordering);
    }

    public void validateComponentsForQuery(List<Object> startComponentValues,
            List<Object> endComponentValues, OrderingMode ordering)
    {
        int indexStart = mapper.validateNoHoleAndReturnLastNonNullIndex(startComponentValues);
        int indexEnd = mapper.validateNoHoleAndReturnLastNonNullIndex(endComponentValues);

        for (int i = 0; i <= Math.min(indexStart, indexEnd); i++)
        {

            //            @SuppressWarnings("unchecked")
            //            Comparable<Object> startValue = (Comparable<Object>) startComponentValues
            //                    .get(i);
            //            Object endValue = endComponentValues.get(i);

            //int comparisonResult = startValue.compareTo(endValue);
            Object startValue = startComponentValues.get(i);
            Object endValue = endComponentValues.get(i);
            int comparisonResult = comparator.compare(startValue, endValue);

            if (ASCENDING.equals(ordering))
            {
                Validator
                        .validateTrue(comparisonResult <= 0,
                                "For slice query with ascending order, start component '"
                                        + startValue
                                        + "' should be lesser or equal to end component '"
                                        + endValue + "'");
                // Stop comparing here
                if (comparisonResult < 0)
                    return;
            }
            else
            {
                Validator
                        .validateTrue(comparisonResult >= 0,
                                "For slice query with descending order, start component '"
                                        + startValue
                                        + "' should be greater or equal to end component '"
                                        + endValue + "'");
                // Stop comparing here
                if (comparisonResult > 0)
                    return;
            }

        }
    }

    private static class ComponentComparator implements Comparator<Object> {

        @Override
        public int compare(Object o1, Object o2) {
            if (o1.getClass().isEnum() && o2.getClass().isEnum())
            {
                String name1 = ((Enum) o1).name();
                String name2 = ((Enum) o2).name();

                return name1.compareTo(name2);
            }
            else if (Comparable.class.isAssignableFrom(o1.getClass())
                    && Comparable.class.isAssignableFrom(o2.getClass()))
            {
                Comparable<Object> comp1 = (Comparable<Object>) o1;
                Comparable<Object> comp2 = (Comparable<Object>) o2;

                return comp1.compareTo(comp2);
            }
            else {
                throw new IllegalArgumentException("Type '" + o1.getClass().getCanonicalName() + "' or type '"
                        + o2.getClass().getCanonicalName() + "' should implements Comparable");
            }
        }
    }
}
