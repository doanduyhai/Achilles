package info.archinnov.achilles.entity;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.EntityMapper;
import info.archinnov.achilles.type.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import me.prettyprint.hector.api.beans.Composite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftEntityMapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityMapper extends EntityMapper
{
    private static final Logger log = LoggerFactory.getLogger(ThriftEntityMapper.class);

    public void setEagerPropertiesToEntity(Object key, List<Pair<Composite, String>> columns,
            EntityMeta entityMeta, Object entity)
    {
        log.trace("Set eager properties to entity {} ", entityMeta.getClassName());

        Map<String, List<Object>> listProperties = new HashMap<String, List<Object>>();
        Map<String, Set<Object>> setProperties = new HashMap<String, Set<Object>>();
        Map<String, Map<Object, Object>> mapProperties = new HashMap<String, Map<Object, Object>>();

        setIdToEntity(key, entityMeta.getIdMeta(), entity);

        Map<String, PropertyMeta<?, ?>> propertyMetas = entityMeta.getPropertyMetas();

        for (Pair<Composite, String> pair : columns)
        {
            String propertyName = pair.left.get(1, STRING_SRZ);

            PropertyMeta<?, ?> propertyMeta = propertyMetas.get(propertyName);

            if (propertyMeta != null)
            {

                switch (propertyMeta.type())
                {
                    case SIMPLE:
                        setSimplePropertyToEntity(pair.right, propertyMeta, entity);
                        break;
                    case LIST:
                        addToList(listProperties, propertyMeta,
                                propertyMeta.getValueFromString(pair.right));
                        break;
                    case SET:
                        addToSet(setProperties, propertyMeta,
                                propertyMeta.getValueFromString(pair.right));
                        break;
                    case MAP:
                        addToMap(mapProperties, propertyMeta,
                                propertyMeta.getKeyValueFromString(pair.right));
                        break;
                    default:
                        log.debug("Property {} is lazy or of proxy type, do not set to entity now");
                        break;
                }
            }
            else
            {
                log.warn("No field mapping for property {}", propertyName);
            }
        }

        setMultiValuesProperties(entity, listProperties, setProperties, mapProperties,
                propertyMetas);
    }

    private void setMultiValuesProperties(Object entity, Map<String, List<Object>> listProperties,
            Map<String, Set<Object>> setProperties, Map<String, Map<Object, Object>> mapProperties,
            Map<String, PropertyMeta<?, ?>> propertyMetas)
    {
        for (Entry<String, List<Object>> entry : listProperties.entrySet())
        {
            setListPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
        }

        for (Entry<String, Set<Object>> entry : setProperties.entrySet())
        {
            setSetPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
        }

        for (Entry<String, Map<Object, Object>> entry : mapProperties.entrySet())
        {
            setMapPropertyToEntity(entry.getValue(), propertyMetas.get(entry.getKey()), entity);
        }
    }
}
