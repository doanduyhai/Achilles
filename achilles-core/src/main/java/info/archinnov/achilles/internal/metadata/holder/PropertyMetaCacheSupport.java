package info.archinnov.achilles.internal.metadata.holder;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class PropertyMetaCacheSupport extends PropertyMetaView{

    private static final Logger log  = LoggerFactory.getLogger(PropertyMetaCacheSupport.class);

    protected PropertyMetaCacheSupport(PropertyMeta meta) {
        super(meta);
    }

    public Set<String> extractClusteredFieldsIfNecessary() {
        log.trace("Get compound primary keys name for property meta {}", meta);
        if (meta.structure().isCompoundPK()) {
            return new HashSet<>(meta.getCompoundPKProperties().getCQLComponentNames());
        } else {
            return Sets.newHashSet(meta.getCQLColumnName());
        }
    }
}
