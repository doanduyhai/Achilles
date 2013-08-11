package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.dao.ThriftAbstractDao.DEFAULT_LENGTH;
import static info.archinnov.achilles.iterator.ThriftAbstractSliceIterator.IteratorType.THRIFT_JOIN_SLICE_ITERATOR;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.execution.SafeExecutionContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftJoinEntityLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.SliceQuery;
import org.apache.cassandra.utils.Pair;

/**
 * ThriftJoinSliceIterator
 * 
 * @author DuyHai DOAN
 * 
 *         Modification of original version from Hector ColumnSliceIterator
 * 
 */
public class ThriftJoinSliceIterator<K, KEY, VALUE> extends
        ThriftAbstractSliceIterator<HColumn<Composite, VALUE>>
{

    private SliceQuery<K, Composite, Object> query;
    private PropertyMeta propertyMeta;
    private ThriftJoinEntityLoader joinHelper = new ThriftJoinEntityLoader();
    private ThriftGenericEntityDao joinEntityDao;

    public ThriftJoinSliceIterator( //
            AchillesConsistencyLevelPolicy policy, //
            ThriftGenericEntityDao joinEntityDao, //
            String cf, PropertyMeta propertyMeta, //
            SliceQuery<K, Composite, Object> query, Composite start, //
            final Composite finish, boolean reversed)
    {
        this(policy, joinEntityDao, cf, propertyMeta, query, start, finish, reversed,
                DEFAULT_LENGTH);
    }

    public ThriftJoinSliceIterator(AchillesConsistencyLevelPolicy policy, //
            ThriftGenericEntityDao joinEntityDao, //
            String cf, PropertyMeta propertyMeta, //
            SliceQuery<K, Composite, Object> query, Composite start, //
            final Composite finish, boolean reversed, int count)
    {
        this(policy, joinEntityDao, cf, propertyMeta, query, start, new ColumnSliceFinish()
        {
            @Override
            public Composite function()
            {
                return finish;
            }
        }, reversed, count);
    }

    public ThriftJoinSliceIterator(AchillesConsistencyLevelPolicy policy, //
            ThriftGenericEntityDao joinEntityDao, //
            String cf, PropertyMeta propertyMeta, //
            SliceQuery<K, Composite, Object> query, Composite start, //
            ColumnSliceFinish finish, boolean reversed)
    {
        this(policy, joinEntityDao, cf, propertyMeta, query, start, finish, reversed,
                DEFAULT_LENGTH);
    }

    public ThriftJoinSliceIterator(AchillesConsistencyLevelPolicy policy, //
            ThriftGenericEntityDao joinEntityDao, //
            String cf, PropertyMeta propertyMeta, //
            SliceQuery<K, Composite, Object> query, Composite start, //
            ColumnSliceFinish finish, boolean reversed, int count)
    {
        super(policy, cf, start, finish, reversed, count);
        this.joinEntityDao = joinEntityDao;
        this.propertyMeta = propertyMeta;
        this.query = query;
        this.query.setRange(this.start, this.finish.function(), this.reversed, this.count);
    }

    @Override
    protected Iterator<HColumn<Composite, VALUE>> fetchData()
    {

        Iterator<HColumn<Composite, Object>> iter = executeWithInitialConsistencyLevel(new SafeExecutionContext<Iterator<HColumn<Composite, Object>>>()
        {
            @Override
            public Iterator<HColumn<Composite, Object>> execute()
            {
                return query.execute().get().getColumns().iterator();
            }
        });

        List<Object> joinIds = new ArrayList<Object>();
        Map<Object, Pair<Composite, Integer>> hColumMap = new HashMap<Object, Pair<Composite, Integer>>();
        List<HColumn<Composite, VALUE>> joinedHColumns = new ArrayList<HColumn<Composite, VALUE>>();

        while (iter.hasNext())
        {
            HColumn<Composite, ?> hColumn = iter.next();

            PropertyMeta joinIdMeta = propertyMeta.joinIdMeta();

            Object joinId;
            if (propertyMeta.isJoin())
            {
                joinId = joinIdMeta.castValue(hColumn.getValue());
            }
            else
            {
                joinId = joinIdMeta.getValueFromString(hColumn.getValue());
            }
            joinIds.add(joinId);
            hColumMap
                    .put(joinId, Pair.create(hColumn.getName(), hColumn.getTtl()));
        }

        if (joinIds.size() > 0)
        {

            Map<Object, VALUE> loadedEntities = joinHelper.loadJoinEntities(
                    (Class<VALUE>) propertyMeta.getValueClass(), joinIds, propertyMeta.joinMeta(), joinEntityDao);

            for (Object joinId : joinIds)
            {
                Pair<Composite, Integer> pair = hColumMap.get(joinId);
                Composite name = pair.left;
                Integer ttl = pair.right;

                HColumn<Composite, VALUE> joinedHColumn = new ThriftHColumn<Composite, VALUE>();

                joinedHColumn.setName(name).setValue(loadedEntities.get(joinId)).setTtl(ttl);
                joinedHColumns.add(joinedHColumn);
            }
        }
        return joinedHColumns.iterator();
    }

    @Override
    public HColumn<Composite, VALUE> next()
    {
        HColumn<Composite, VALUE> column = iterator.next();
        start = column.getName();
        columns++;

        return column;
    }

    @Override
    public void remove()
    {
        iterator.remove();
    }

    @Override
    protected void changeQueryRange()
    {
        query.setRange(start, finish.function(), reversed, count);
    }

    @Override
    protected void resetStartColumn(HColumn<Composite, VALUE> column)
    {
        start = column.getName();
    }

    @Override
    public IteratorType type()
    {
        return THRIFT_JOIN_SLICE_ITERATOR;
    }
}
