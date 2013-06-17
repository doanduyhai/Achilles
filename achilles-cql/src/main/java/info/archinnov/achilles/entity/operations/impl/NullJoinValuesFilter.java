package info.archinnov.achilles.entity.operations.impl;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import java.util.List;
import org.apache.cassandra.utils.Pair;
import com.google.common.base.Predicate;

public class NullJoinValuesFilter implements Predicate<Pair<List<?>, PropertyMeta<?, ?>>> {

    @Override
    public boolean apply(Pair<List<?>, PropertyMeta<?, ?>> joinValuesPair)
    {
        return !joinValuesPair.left.isEmpty();
    }
}
