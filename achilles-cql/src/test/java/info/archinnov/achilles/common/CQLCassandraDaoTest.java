package info.archinnov.achilles.common;

import static info.archinnov.achilles.configuration.CQLConfigurationParameters.*;
import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_TABLE;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.entity.manager.CQLEntityManagerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

/**
 * CQLCassandraDaoTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLCassandraDaoTest extends AbstractCassandraDaoTest {
    private static final String ENTITY_PACKAGE = "info.archinnov.achilles.test.integration.entity";

    private static Cluster cluster;
    private static Session session;
    private static CQLEntityManagerFactory emf;
    private static CQLEntityManager em;

    static {
        String cassandraHost = System.getProperty(CASSANDRA_HOST);
        if (StringUtils.isNotBlank(cassandraHost) && cassandraHost.contains(":")) {
            String[] fullHostName = StringUtils.split(cassandraHost, ":");

            assert fullHostName.length == 2;

            cluster = Cluster.builder() //
                    .addContactPoints(fullHostName[0]).withPort(Integer.parseInt(fullHostName[1])).build();

        } else {
            cluster = Cluster.builder() //
                    .addContactPoints(CASSANDRA_TEST_HOST).withPort(CASSANDRA_CQL_TEST_PORT).build();
        }
        session = cluster.connect(CASSANDRA_KEYSPACE_NAME);
        Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(ENTITY_PACKAGES_PARAM, ENTITY_PACKAGE);
        configMap.put(CONNECTION_CONTACT_POINTS_PARAM, CASSANDRA_TEST_HOST);
        configMap.put(CONNECTION_PORT_PARAM, CASSANDRA_CQL_TEST_PORT + "");
        configMap.put(KEYSPACE_NAME_PARAM, CASSANDRA_KEYSPACE_NAME);
        configMap.put(FORCE_CF_CREATION_PARAM, true);
        configMap.put(ENSURE_CONSISTENCY_ON_JOIN_PARAM, true);

        //createTables();
        emf = new CQLEntityManagerFactory(configMap);
        em = emf.createEntityManager();
    }

    public static com.datastax.driver.core.Cluster getCqlCluster() {
        return cluster;
    }

    public static Session getCqlSession() {
        return session;
    }

    public static int getCqlPort() {
        return CASSANDRA_CQL_TEST_PORT;
    }

    public static CQLEntityManager getEm() {
        return em;
    }

    private static void createTables() {
        StringBuilder tableCompleteBean = new StringBuilder();
        tableCompleteBean.append("CREATE TABLE completebean(");
        tableCompleteBean.append("id bigint,");
        tableCompleteBean.append("name text,");
        tableCompleteBean.append("label text,");
        tableCompleteBean.append("age_in_years bigint,");
        tableCompleteBean.append("welcometweet uuid,");
        tableCompleteBean.append("friends list<text>,");
        tableCompleteBean.append("followers set<text>,");
        tableCompleteBean.append("preferences map<int,text>,");
        tableCompleteBean.append("favoritetweets list<text>,");
        tableCompleteBean.append("primary key(id))");

        StringBuilder tableTweet = new StringBuilder();
        tableTweet.append("CREATE TABLE tweet(");
        tableTweet.append("id uuid,");
        tableTweet.append("creator bigint,");
        tableTweet.append("content text,");
        tableTweet.append("primary key(id))");

        StringBuilder tableCompleteBeanTweets = new StringBuilder();
        tableCompleteBeanTweets.append("CREATE TABLE complete_bean_tweets(");
        tableCompleteBeanTweets.append("id bigint,");
        tableCompleteBeanTweets.append("key uuid,");
        tableCompleteBeanTweets.append("value text,");
        tableCompleteBeanTweets.append("primary key(id,key))");

        StringBuilder tableCompleteBeanUserTweets = new StringBuilder();
        tableCompleteBeanUserTweets.append("CREATE TABLE complete_bean_user_tweets(");
        tableCompleteBeanUserTweets.append("id bigint,");
        tableCompleteBeanUserTweets.append("user text,");
        tableCompleteBeanUserTweets.append("tweet uuid,");
        tableCompleteBeanUserTweets.append("value text,");
        tableCompleteBeanUserTweets.append("primary key(id,user,tweet))");

        StringBuilder tableCompleteBeanWideMap = new StringBuilder();
        tableCompleteBeanWideMap.append("CREATE TABLE complete_bean_widemap(");
        tableCompleteBeanWideMap.append("id bigint,");
        tableCompleteBeanWideMap.append("key int,");
        tableCompleteBeanWideMap.append("value text,");
        tableCompleteBeanWideMap.append("primary key(id,key))");

        StringBuilder tableCompleteBeanMultiKeyWideMap = new StringBuilder();
        tableCompleteBeanMultiKeyWideMap.append("CREATE TABLE complete_bean_multi_key_widemap(");
        tableCompleteBeanMultiKeyWideMap.append("id bigint,");
        tableCompleteBeanMultiKeyWideMap.append("user text,");
        tableCompleteBeanMultiKeyWideMap.append("tweet uuid,");
        tableCompleteBeanMultiKeyWideMap.append("value text,");
        tableCompleteBeanMultiKeyWideMap.append("primary key(id,user,tweet))");

        StringBuilder tableCompleteBeanPopularTopics = new StringBuilder();
        tableCompleteBeanPopularTopics.append("CREATE TABLE complete_bean_popular_topics(");
        tableCompleteBeanPopularTopics.append("id bigint,");
        tableCompleteBeanPopularTopics.append("key text,");
        tableCompleteBeanPopularTopics.append("value counter,");
        tableCompleteBeanPopularTopics.append("primary key(id,key))");

        StringBuilder tableAchillesCounter = new StringBuilder();
        tableAchillesCounter.append("CREATE TABLE ").append(CQL_COUNTER_TABLE).append("(");
        tableAchillesCounter.append("fqcn text,");
        tableAchillesCounter.append("primary_key text,");
        tableAchillesCounter.append("property_name text,");
        tableAchillesCounter.append("counter_value counter,");
        tableAchillesCounter.append("primary key((fqcn,primary_key),property_name))");

        StringBuilder tableClusteredTweet = new StringBuilder();
        tableClusteredTweet.append("CREATE TABLE clusteredtweet(");
        tableClusteredTweet.append("user_id bigint,");
        tableClusteredTweet.append("tweet_id uuid,");
        tableClusteredTweet.append("creation_date timestamp,");
        tableClusteredTweet.append("content text,");
        tableClusteredTweet.append("original_author_id bigint,");
        tableClusteredTweet.append("is_a_retweet boolean,");
        tableClusteredTweet.append("primary key(user_id,tweet_id,creation_date))");

        StringBuilder tableClusteredMessage = new StringBuilder();
        tableClusteredMessage.append("CREATE TABLE clusteredmessage(");
        tableClusteredMessage.append("id bigint,");
        tableClusteredMessage.append("type text,");
        tableClusteredMessage.append("label text,");
        tableClusteredMessage.append("primary key(id,type))");

        session.execute(tableClusteredMessage.toString());
        session.execute(tableClusteredTweet.toString());
        session.execute(tableAchillesCounter.toString());
        session.execute(tableTweet.toString());
        session.execute(tableCompleteBean.toString());
        session.execute(tableCompleteBeanTweets.toString());
        session.execute(tableCompleteBeanUserTweets.toString());
        session.execute(tableCompleteBeanWideMap.toString());
        session.execute(tableCompleteBeanMultiKeyWideMap.toString());
        session.execute(tableCompleteBeanPopularTopics.toString());

        String tableCQLUser = "create table cql3_user(id bigint,firstname text,lastname text, age int, primary key(id))";
        String tableList = "create table cql3_list(id bigint,myList list<text>, primary key(id))";
        String tableSet = "create table cql3_set(id bigint,mySet set<text>, primary key(id))";
        String tableMap = "create table cql3_map(id bigint,myMap map<int,text>, primary key(id))";
        String wideRow = "create table widerow(id bigint,key text,value text, primary key(id,key))";
        String clustering = "create table clustering(a int,b int,c int,d int, primary key (a,b,c))";
        String counter = "create table achillescounter(fqcn text,pk text,key text,counter_value counter, primary key ((fqcn,pk),key))";
        session.execute(tableCQLUser);
        session.execute(tableList);
        session.execute(tableSet);
        session.execute(tableMap);
        session.execute(wideRow);
        session.execute(clustering);
        session.execute(counter);
    }

    public static void truncateTables() {
        String listAllTables = "select columnfamily_name from system.schema_columnfamilies where keyspace_name='achilles'";
        List<Row> rows = session.execute(listAllTables).all();

        for (Row row : rows) {
            String tableName = row.getString("columnfamily_name");
            session.execute(new SimpleStatement("truncate " + tableName));
        }
    }

    public static void truncateTable(String tableName) {
        session.execute(new SimpleStatement("truncate " + tableName));
    }
}
