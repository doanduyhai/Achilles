package info.archinnov.achilles.compound;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public abstract class CompoundKeyValidator
{
    protected ComponentComparator comparator = new ComponentComparator();

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

        validateNoHoleAndReturnLastNonNullIndex(Arrays.<Object> asList(clusteringKeys));

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

        validateComponentsForSliceQuery(start.subList(0, start.size()),
                end.subList(0, end.size()), ordering);
    }

    public int validateNoHoleAndReturnLastNonNullIndex(List<Object> components)
    {
        boolean nullFlag = false;
        int lastNotNullIndex = 0;
        for (Object keyValue : components)
        {
            if (keyValue != null)
            {
                if (nullFlag)
                {
                    throw new IllegalArgumentException(
                            "There should not be any null value between two non-null components of a @CompoundKey");
                }
                lastNotNullIndex++;
            }
            else
            {
                nullFlag = true;
            }
        }
        lastNotNullIndex--;

        return lastNotNullIndex;
    }

    public int getLastNonNullIndex(List<Object> components)
    {
        for (int i = 0; i < components.size(); i++)
        {
            if (components.get(i) == null)
            {
                return i - 1;
            }
        }
        return components.size() - 1;
    }

    public abstract void validateComponentsForSliceQuery(List<Object> startComponents,
            List<Object> endComponents, OrderingMode ordering);

    protected static class ComponentComparator implements Comparator<Object> {

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
