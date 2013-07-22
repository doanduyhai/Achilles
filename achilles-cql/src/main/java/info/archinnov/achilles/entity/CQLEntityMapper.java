package info.archinnov.achilles.entity;

import info.archinnov.achilles.compound.CQLCompoundKeyMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.EntityMapper;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;
import com.datastax.driver.core.Row;

/**
 * CQLEntityMapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityMapper extends EntityMapper
{

    private CQLRowMethodInvoker cqlRowInvoker = new CQLRowMethodInvoker();
    private CQLCompoundKeyMapper compoundKeyMapper = new CQLCompoundKeyMapper();

    public void setEagerPropertiesToEntity(Row row, EntityMeta entityMeta, Object entity)
    {
        for (PropertyMeta<?, ?> pm : entityMeta.getEagerMetas())
        {
            setPropertyToEntity(row, pm, entity);
        }
    }

    public void setPropertyToEntity(Row row, PropertyMeta<?, ?> pm, Object entity)
    {
        if (row != null)
        {
            if (pm.isCompound())
            {
                Object compoundKey = compoundKeyMapper.createFromRow(row, pm);
                invoker.setValueToField(entity, pm.getSetter(), compoundKey);
            }
            else {
                String propertyName = pm.getPropertyName();
                if (!row.isNull(propertyName))
                {
                    Object value = cqlRowInvoker.invokeOnRowForFields(row, pm);
                    invoker.setValueToField(entity, pm.getSetter(), value);
                }
            }
        }
    }

    public void setJoinValueToEntity(Object value, PropertyMeta<?, ?> pm, Object entity)
    {
        invoker.setValueToField(entity, pm.getSetter(), value);
    }
}
