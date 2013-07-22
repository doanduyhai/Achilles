package info.archinnov.achilles.query;

import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.compound.ThriftCompoundKeyValidator;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.OrderingMode;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftQueryValidator
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftQueryValidator
{
    private static final Logger log = LoggerFactory.getLogger(ThriftQueryValidator.class);
    private ThriftCompoundKeyValidator compoundKeyValidator = new ThriftCompoundKeyValidator();
    private ThriftCompoundKeyMapper mapper = new ThriftCompoundKeyMapper();

    public <K> void validateBoundsForQuery(PropertyMeta<?, ?> propertyMeta, K start, K end,
            OrderingMode ordering)
    {
        log.trace("Check composites {} / {} with respect to ordering mode {}", start, end,
                ordering.name());
        if (start != null && end != null)
        {
            if (propertyMeta.isCompound())
            {
                List<Method> componentGetters = propertyMeta.getComponentGetters();
                List<Object> startComponents = mapper.fromCompoundToComponents(start, componentGetters);
                List<Object> endComponents = mapper.fromCompoundToComponents(end, componentGetters);
                compoundKeyValidator.validateComponentsForQuery(startComponents, endComponents, ordering);
            }
            else
            {
                compoundKeyValidator.validateComponentsForQuery(Arrays.<Object> asList(start),
                        Arrays.<Object> asList(end),
                        ordering);
            }
        }
    }
}
