package info.archinnov.achilles.composite;

import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.ThriftHColumn;
import java.util.List;
import java.util.Map;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;

/**
 * ThriftCompositeTransformer
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCompositeTransformer
{
    private static final Logger log = LoggerFactory.getLogger(ThriftCompositeTransformer.class);

    private ThriftCompoundKeyMapper compoundKeyMapper = new ThriftCompoundKeyMapper();
    private ThriftEntityMapper mapper = new ThriftEntityMapper();

    public Function<HColumn<Composite, ?>, ?> buildRawValueTransformer()
    {
        return new Function<HColumn<Composite, ?>, Object>()
        {
            @Override
            public Object apply(HColumn<Composite, ?> hColumn)
            {
                return hColumn.getValue();
            }
        };
    }

    // //////////////////// Clustered Entities

    public <T> Function<HColumn<Composite, Object>, T> buildClusteredEntityTransformer(
            final Class<T> entityClass,
            final ThriftPersistenceContext context)
    {
        return new Function<HColumn<Composite, Object>, T>()
        {
            @Override
            public T apply(HColumn<Composite, Object> hColumn)
            {
                return buildClusteredEntity(entityClass, context, hColumn);
            }
        };
    }

    public <T> Function<HColumn<Composite, Object>, T> buildJoinClusteredEntityTransformer(
            final Class<T> entityClass, final ThriftPersistenceContext context,
            final Map<Object, Object> joinEntitiesMap)
    {
        return new Function<HColumn<Composite, Object>, T>()
        {
            @Override
            public T apply(HColumn<Composite, Object> hColumn)
            {
                Object joinId = hColumn.getValue();
                Object clusteredValue = joinEntitiesMap.get(joinId);
                ThriftHColumn<Composite, Object> hCol = new ThriftHColumn<Composite, Object>(
                        hColumn.getName(), clusteredValue);
                return buildClusteredEntity(entityClass, context, hCol);
            }
        };
    }

    public <T> Function<HCounterColumn<Composite>, T> buildCounterClusteredEntityTransformer(
            final Class<T> entityClass, final ThriftPersistenceContext context)
    {
        return new Function<HCounterColumn<Composite>, T>()
        {
            @Override
            public T apply(HCounterColumn<Composite> hColumn)
            {
                return buildCounterClusteredEntity(entityClass, context, hColumn);
            }
        };
    }

    public <T> T buildClusteredEntity(Class<T> entityClass, ThriftPersistenceContext context,
            HColumn<Composite, Object> hColumn)
    {
        PropertyMeta<?, ?> idMeta = context.getIdMeta();
        PropertyMeta<?, ?> pm = context.getFirstMeta();
        Object embeddedId = buildEmbeddedIdFromComponents(context, hColumn
                .getName()
                .getComponents());
        Object clusteredValue = hColumn.getValue();
        Object value = pm.castValue(clusteredValue);
        return mapper.createClusteredEntityWithValue(entityClass, idMeta, pm, embeddedId, value);
    }

    public <T> T buildCounterClusteredEntity(Class<T> entityClass,
            ThriftPersistenceContext context,
            HCounterColumn<Composite> hColumn)
    {
        PropertyMeta<?, ?> idMeta = context.getIdMeta();
        Object embeddedId = buildEmbeddedIdFromComponents(context, hColumn
                .getName()
                .getComponents());
        return mapper.initClusteredEntity(entityClass, idMeta, embeddedId);
    }

    private Object buildEmbeddedIdFromComponents(ThriftPersistenceContext context,
            List<Component<?>> components)
    {
        Object partitionKey = context.getPartitionKey();
        PropertyMeta<?, ?> idMeta = context.getIdMeta();
        return compoundKeyMapper.fromCompositeToEmbeddedId(idMeta, components, partitionKey);
    }

}
