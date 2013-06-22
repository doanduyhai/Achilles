package testBuilders;

import java.util.Arrays;
import java.util.List;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.commons.collections.CollectionUtils;

/**
 * CompositeTestBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CompositeTestBuilder
{

    private List<?> values;
    private ComponentEquality equality;

    public static CompositeTestBuilder builder()
    {
        return new CompositeTestBuilder();
    }

    public Composite buildForQuery()
    {
        Composite built = new Composite();
        if (!CollectionUtils.isEmpty(values))
        {
            for (int i = 0; i < values.size(); i++)
            {
                ComponentEquality eq;
                if (i != values.size() - 1)
                {
                    eq = ComponentEquality.EQUAL;
                }
                else
                {
                    eq = equality;

                }
                built.addComponent(i, values.get(i), eq);
            }
        }
        return built;
    }

    public Composite buildSimple()
    {
        Composite built = new Composite();
        if (!CollectionUtils.isEmpty(values))
        {
            for (int i = 0; i < values.size(); i++)
            {
                Object value = values.get(i);
                built.setComponent(i, value, SerializerTypeInferer.getSerializer(value));
            }
        }
        return built;
    }

    public CompositeTestBuilder values(Object... values)
    {
        this.values = Arrays.asList(values);
        return this;
    }

    public CompositeTestBuilder equality(ComponentEquality equality)
    {
        this.equality = equality;
        return this;
    }

    public CompositeTestBuilder gt()
    {
        this.equality = ComponentEquality.GREATER_THAN_EQUAL;
        return this;
    }

    public CompositeTestBuilder lt()
    {
        this.equality = ComponentEquality.LESS_THAN_EQUAL;
        return this;
    }
}
