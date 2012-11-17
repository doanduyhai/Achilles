package fr.doan.achilles.parser;

import java.util.ArrayList;
import java.util.List;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.util.ClassUtils;
import org.springframework.util.SystemPropertyUtils;

public class EntityExplorer
{

	public List<Class<?>> discoverEntities(String packageName)
	{
		return this.findMyTypes(packageName);
	}

	public List<Class<?>> discoverEntities(List<String> packageNames)
	{
		List<Class<?>> candidates = new ArrayList<Class<?>>();
		for (String packageName : packageNames)
		{
			candidates.addAll(this.findMyTypes(packageName));
		}
		return candidates;
	}

	private List<Class<?>> findMyTypes(String basePackage)
	{
		ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
		MetadataReaderFactory metadataReaderFactory = new CachingMetadataReaderFactory(resourcePatternResolver);

		List<Class<?>> candidates = new ArrayList<Class<?>>();
		String packageSearchPath = ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX + resolveBasePackage(basePackage) + "/" + "**/*.class";
		try
		{
			Resource[] resources = resourcePatternResolver.getResources(packageSearchPath);
			for (Resource resource : resources)
			{
				if (resource.isReadable())
				{
					MetadataReader metadataReader = metadataReaderFactory.getMetadataReader(resource);
					if (isCandidate(metadataReader))
					{
						candidates.add(Class.forName(metadataReader.getClassMetadata().getClassName()));
					}
				}
			}
		}
		catch (Exception ex)
		{}
		return candidates;
	}

	private String resolveBasePackage(String basePackage)
	{
		return ClassUtils.convertClassNameToResourcePath(SystemPropertyUtils.resolvePlaceholders(basePackage));
	}

	private boolean isCandidate(MetadataReader metadataReader)
	{
		try
		{
			Class<?> c = Class.forName(metadataReader.getClassMetadata().getClassName());
			if (c.getAnnotation(javax.persistence.Table.class) != null)
			{
				return true;
			}
		}
		catch (Throwable e)
		{}
		return false;
	}
}
