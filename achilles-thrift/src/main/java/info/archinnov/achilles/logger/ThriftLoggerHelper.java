package info.archinnov.achilles.logger;

import info.archinnov.achilles.helper.LoggerHelper;
import info.archinnov.achilles.serializer.ThriftSerializerUtils;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Function;

/**
 * ThriftLoggerHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftLoggerHelper extends LoggerHelper
{
	public static Function<Serializer<?>, String> srzToStringFn = new Function<Serializer<?>, String>()
	{
		public String apply(Serializer<?> srz)
		{
			return srz.getComparatorType().getTypeName();
		}
	};

	public static String format(Composite comp)
	{
		String formatted = "null";
		if (comp != null)
		{
			formatted = format(comp.getComponents());
		}
		return formatted;
	}

	public static String format(List<Component<?>> components)
	{
		String formatted = "[]";
		if (components != null && components.size() > 0)
		{
			List<String> componentsText = new ArrayList<String>();
			int componentNb = components.size();
			for (int i = 0; i < componentNb; i++)
			{
				Component<?> component = components.get(i);
				String componentValue;
				if (component.getSerializer() == ThriftSerializerUtils.BYTE_SRZ)
				{
					componentValue = ByteBufferUtil.getArray(component.getBytes())[0] + "";
				}
				else
				{
					componentValue = component.getValue(component.getSerializer()).toString();
				}

				if (i == componentNb - 1)
				{
					componentsText.add(componentValue + "(" + component.getEquality().name() + ")");
				}
				else
				{
					componentsText.add(componentValue);
				}
			}
			formatted = '[' + StringUtils.join(componentsText, ':') + ']';;
		}
		return formatted;
	}
}
