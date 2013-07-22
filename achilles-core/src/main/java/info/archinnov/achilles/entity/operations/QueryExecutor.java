package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.query.SliceQuery;
import java.util.Iterator;
import java.util.List;

/**
 * QueryExecutor
 *
 * @author DuyHai DOAN
 *
 */
public interface QueryExecutor {

    public <T> List<T> get(SliceQuery<T> query);

    public <T> Iterator<T> iterator(SliceQuery<T> query);

    public <T> void remove(SliceQuery<T> query);

}
