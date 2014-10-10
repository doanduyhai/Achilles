package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.Strategy;
import info.archinnov.achilles.type.NamingStrategy;

@Entity(table = "caseSensitiveNaming")
@Strategy(naming = NamingStrategy.CASE_SENSITIVE)
public class ClusteredEntityWithNamingStrategy {

    @EmbeddedId
    private EmbeddedKey embeddedKey;

    @Column
    private String firstName;

    @Column(name = "last_name")
    private String lastName;

    public ClusteredEntityWithNamingStrategy() {
    }

    public ClusteredEntityWithNamingStrategy(Long partitionKey, String clusteringKey, String firstName, String lastName) {
        this.embeddedKey = new EmbeddedKey(partitionKey, clusteringKey);
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public EmbeddedKey getEmbeddedKey() {
        return embeddedKey;
    }

    public void setEmbeddedKey(EmbeddedKey embeddedKey) {
        this.embeddedKey = embeddedKey;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public static class EmbeddedKey {

        @Order(1)
        private Long partitionKey;

        @Order(2)
        @Column(name = "clustering")
        private String clusteringKey;

        public EmbeddedKey() {
        }

        public EmbeddedKey(Long partitionKey, String clusteringKey) {
            this.partitionKey = partitionKey;
            this.clusteringKey = clusteringKey;
        }

        public Long getPartitionKey() {
            return partitionKey;
        }

        public void setPartitionKey(Long partitionKey) {
            this.partitionKey = partitionKey;
        }

        public String getClusteringKey() {
            return clusteringKey;
        }

        public void setClusteringKey(String clusteringKey) {
            this.clusteringKey = clusteringKey;
        }
    }

}
