package info.archinnov.achilles.query;

import static info.archinnov.achilles.type.OrderingMode.ASCENDING;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.compound.ThriftCompoundKeyValidator;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;
import java.lang.reflect.Method;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WideMapQueryValidator
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
            if (propertyMeta.isSingleKey())
            {
                @SuppressWarnings("unchecked")
                Comparable<K> startComp = (Comparable<K>) start;

                if (ASCENDING.equals(ordering))
                {
                    Validator.validateTrue(startComp.compareTo(end) <= 0,
                            "For range query, start value should be lesser or equal to end value");
                }
                else
                {
                    Validator
                            .validateTrue(startComp.compareTo(end) >= 0,
                                    "For reverse range query, start value should be greater or equal to end value");
                }
            }
            else
            {
                List<Method> componentGetters = propertyMeta.getComponentGetters();

                List<Object> startComponents = mapper.fromCompoundToComponents(start,
                        componentGetters);
                List<Object> endComponents = mapper.fromCompoundToComponents(end,
                        componentGetters);

                compoundKeyValidator.validateComponentsForQuery(startComponents, endComponents,
                        ordering);
            }
        }
    }
}
