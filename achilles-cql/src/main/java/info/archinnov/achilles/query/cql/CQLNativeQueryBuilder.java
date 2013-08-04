package info.archinnov.achilles.query.cql;

import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.entity.operations.CQLNativeQueryMapper;
import java.util.List;
import java.util.Map;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

/**
 * CQLNativeQuery
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLNativeQueryBuilder {

    private CQLDaoContext daoContext;
    private String queryString;

    private CQLNativeQueryMapper mapper = new CQLNativeQueryMapper();

    public CQLNativeQueryBuilder(CQLDaoContext daoContext, String queryString) {
        this.daoContext = daoContext;
        this.queryString = queryString;
    }

    /**
     * Return found rows.
     * The list represents the number of returned rows
     * The map contains the (column name, column value) of each row.
     * The map is backed by a LinkedHashMap and thus preserves the columns order as they were declared in the native
     * query
     * 
     * @return List<Map<String, Object>>
     */
    public List<Map<String, Object>> get()
    {
        List<Row> rows = daoContext.execute(new SimpleStatement(queryString)).all();
        return mapper.mapRows(rows);
    }

    /**
     * Return the first found row.
     * The map contains the (column name, column value) of each row.
     * The map is backed by a LinkedHashMap and thus preserves the columns order as they were declared in the native
     * query
     * 
     * @return Map<String, Object>
     */
    public Map<String, Object> first()
    {
        List<Row> rows = daoContext.execute(new SimpleStatement(queryString)).all();
        List<Map<String, Object>> result = mapper.mapRows(rows);
        if (result.isEmpty())
            return null;
        else
            return result.get(0);
    }
}
