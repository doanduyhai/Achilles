package info.archinnov.achilles.entity.parsing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityExplorer
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityExplorer {
    private static final Logger log = LoggerFactory.getLogger(EntityExplorer.class);

    public List<Class<?>> discoverEntities(List<String> packageNames) throws ClassNotFoundException, IOException {
        log.debug("Discovery of Achilles entity classes in packages {}", StringUtils.join(packageNames, ","));

        Set<Class<?>> candidateClasses = new HashSet<Class<?>>();
        Reflections reflections = new Reflections(packageNames);
        candidateClasses.addAll(reflections.getTypesAnnotatedWith(javax.persistence.Entity.class));
        candidateClasses.addAll(reflections.getTypesAnnotatedWith(javax.persistence.Table.class));
        return new ArrayList<Class<?>>(candidateClasses);
    }

}
