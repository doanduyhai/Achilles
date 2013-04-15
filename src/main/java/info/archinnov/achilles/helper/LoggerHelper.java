package info.archinnov.achilles.helper;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.commons.lang.StringUtils;

/**
 * LoggerHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class LoggerHelper
{
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
				if (i == componentNb - 1)
				{
					componentsText.add(component.getValue(component.getSerializer()).toString()
							+ "(" + component.getEquality().name() + ")");
				}
				else
				{
					componentsText.add(component.getValue(component.getSerializer()).toString());
				}
			}
			formatted = '[' + StringUtils.join(componentsText, ':') + ']';;
		}
		return formatted;
	}
}
