package info.archinnov.achilles.integration.spring;

import static info.archinnov.achilles.configuration.CQLConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static org.apache.commons.lang.StringUtils.isBlank;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.entity.manager.CQLEntityManagerFactory;
import info.archinnov.achilles.json.ObjectMapperFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.FactoryBean;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * CQLEntityManagerFactoryBean
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityManagerFactoryBean implements FactoryBean<CQLEntityManager>
{
    private String entityPackages;

    private String contactPoints;
    private Integer port;
    private String keyspaceName;
    private Compression compression;
    private RetryPolicy retryPolicy;
    private LoadBalancingPolicy loadBalancingPolicy;
    private ReconnectionPolicy reconnectionPolicy;
    private String username;
    private String password;
    private Boolean disableJmx;
    private Boolean disableMetrics;
    private Boolean sslEnabled;
    private SSLOptions sslOptions;

    private ObjectMapperFactory objectMapperFactory;
    private ObjectMapper objectMapper;

    private String consistencyLevelReadDefault;
    private String consistencyLevelWriteDefault;
    private Map<String, String> consistencyLevelReadMap;
    private Map<String, String> consistencyLevelWriteMap;

    private boolean forceColumnFamilyCreation = false;
    private boolean ensureJoinConsistency = false;

    private CQLEntityManager em;

    public void initialize()
    {
        Map<String, Object> configMap = new HashMap<String, Object>();

        fillEntityPackages(configMap);

        fillCluster(configMap);

        fillCompression(configMap);

        fillPolicies(configMap);

        fillCredentials(configMap);

        fillJmxAndMetrics(configMap);

        fillSSLConfig(configMap);

        fillObjectMapper(configMap);

        fillConsistencyLevels(configMap);

        configMap.put(FORCE_CF_CREATION_PARAM, forceColumnFamilyCreation);
        configMap.put(ENSURE_CONSISTENCY_ON_JOIN_PARAM, ensureJoinConsistency);

        CQLEntityManagerFactory factory = new CQLEntityManagerFactory(configMap);
        em = factory.createEntityManager();
    }

    private void fillEntityPackages(Map<String, Object> configMap)
    {
        if (isBlank(entityPackages))
        {
            throw new IllegalArgumentException(
                    "'entityPackages' should be provided for entity scanning");
        }
        configMap.put(ENTITY_PACKAGES_PARAM, entityPackages);
    }

    private void fillCluster(Map<String, Object> configMap)
    {
        if (isBlank(contactPoints) || port == null)
        {
            throw new IllegalArgumentException(
                    "'contactPoints' and 'port' for Cassandra connection should be provided");
        }
        configMap.put(CONNECTION_CONTACT_POINTS_PARAM, contactPoints);
        configMap.put(CONNECTION_PORT_PARAM, port);

        if (isBlank(keyspaceName))
        {
            throw new IllegalArgumentException(
                    "'keyspaceName' for Cassandra connection should be provided");
        }
        configMap.put(KEYSPACE_NAME_PARAM, keyspaceName);

    }

    private void fillCompression(Map<String, Object> configMap)
    {
        if (compression != null)
        {
            configMap.put(COMPRESSION_TYPE, compression);
        }
    }

    private void fillPolicies(Map<String, Object> configMap)
    {
        if (retryPolicy != null)
        {
            configMap.put(RETRY_POLICY, retryPolicy);
        }

        if (loadBalancingPolicy != null)
        {
            configMap.put(LOAD_BALANCING_POLICY, loadBalancingPolicy);
        }

        if (reconnectionPolicy != null)
        {
            configMap.put(RECONNECTION_POLICY, reconnectionPolicy);
        }
    }

    private void fillJmxAndMetrics(Map<String, Object> configMap)
    {
        if (disableJmx != null)
        {
            configMap.put(DISABLE_JMX, disableJmx);
        }

        if (disableMetrics != null)
        {
            configMap.put(DISABLE_METRICS, disableMetrics);
        }
    }

    private void fillCredentials(Map<String, Object> configMap)
    {
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password))
        {
            configMap.put(USERNAME, username);
            configMap.put(PASSWORD, password);
        }
    }

    private void fillSSLConfig(Map<String, Object> configMap)
    {
        if (sslEnabled != null)
        {
            if (sslOptions == null)
                throw new IllegalArgumentException("'sslOptions' property should be set when SSL is enabled");

            configMap.put(SSL_ENABLED, sslEnabled);
            configMap.put(SSL_OPTIONS, sslOptions);
        }
    }

    private void fillObjectMapper(Map<String, Object> configMap)
    {
        if (objectMapperFactory != null)
        {
            configMap.put(OBJECT_MAPPER_FACTORY_PARAM, objectMapperFactory);
        }
        if (objectMapper != null)
        {
            configMap.put(OBJECT_MAPPER_PARAM, objectMapper);
        }
    }

    private void fillConsistencyLevels(Map<String, Object> configMap)
    {
        if (consistencyLevelReadDefault != null)
        {
            configMap.put(CONSISTENCY_LEVEL_READ_DEFAULT_PARAM, consistencyLevelReadDefault);
        }
        if (consistencyLevelWriteDefault != null)
        {
            configMap.put(CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM, consistencyLevelWriteDefault);
        }

        if (consistencyLevelReadMap != null)
        {
            configMap.put(CONSISTENCY_LEVEL_READ_MAP_PARAM, consistencyLevelReadMap);
        }
        if (consistencyLevelWriteMap != null)
        {
            configMap.put(CONSISTENCY_LEVEL_WRITE_MAP_PARAM, consistencyLevelWriteMap);
        }
    }

    public void setContactPoints(String contactPoints) {
        this.contactPoints = contactPoints;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public void setKeyspaceName(String keyspaceName)
    {
        this.keyspaceName = keyspaceName;
    }

    public void setCompression(Compression compression) {
        this.compression = compression;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
        this.loadBalancingPolicy = loadBalancingPolicy;
    }

    public void setReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
        this.reconnectionPolicy = reconnectionPolicy;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDisableJmx(Boolean disableJmx) {
        this.disableJmx = disableJmx;
    }

    public void setDisableMetrics(Boolean disableMetrics) {
        this.disableMetrics = disableMetrics;
    }

    public void setSslEnabled(Boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public void setSslOptions(SSLOptions sslOptions) {
        this.sslOptions = sslOptions;
    }

    public void setEntityPackages(String entityPackages)
    {
        this.entityPackages = entityPackages;
    }

    public void setForceColumnFamilyCreation(boolean forceColumnFamilyCreation)
    {
        this.forceColumnFamilyCreation = forceColumnFamilyCreation;
    }

    public void setObjectMapperFactory(ObjectMapperFactory objectMapperFactory)
    {
        this.objectMapperFactory = objectMapperFactory;
    }

    public void setObjectMapper(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    public void setConsistencyLevelReadDefault(String consistencyLevelReadDefault)
    {
        this.consistencyLevelReadDefault = consistencyLevelReadDefault;
    }

    public void setConsistencyLevelWriteDefault(String consistencyLevelWriteDefault)
    {
        this.consistencyLevelWriteDefault = consistencyLevelWriteDefault;
    }

    public void setConsistencyLevelReadMap(Map<String, String> consistencyLevelReadMap)
    {
        this.consistencyLevelReadMap = consistencyLevelReadMap;
    }

    public void setConsistencyLevelWriteMap(Map<String, String> consistencyLevelWriteMap)
    {
        this.consistencyLevelWriteMap = consistencyLevelWriteMap;
    }

    public void setEnsureJoinConsistency(boolean ensureJoinConsistency)
    {
        this.ensureJoinConsistency = ensureJoinConsistency;
    }

    public CQLEntityManager getObject() throws Exception
    {
        return em;
    }

    @Override
    public Class<?> getObjectType()
    {
        return CQLEntityManager.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }

}
