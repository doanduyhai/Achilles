package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.type.NamingStrategy;

@Entity(table = "caseSensitiveNaming")
@Strategy(naming = NamingStrategy.CASE_SENSITIVE)
public class ClusteredEntityWithNamingStrategy {

    @CompoundPrimaryKey
    private CompoundPK compoundPK;

    @Column
    private String firstName;

    @Column(name = "last_name")
    private String lastName;

    public ClusteredEntityWithNamingStrategy() {
    }

    public ClusteredEntityWithNamingStrategy(Long partitionKey, String clusteringKey, String firstName, String lastName) {
        this.compoundPK = new CompoundPK(partitionKey, clusteringKey);
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public CompoundPK getCompoundPK() {
        return compoundPK;
    }

    public void setCompoundPK(CompoundPK compoundPK) {
        this.compoundPK = compoundPK;
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

    public static class CompoundPK {

        @PartitionKey
        private Long partitionKey;

        @ClusteringColumn
        @Column(name = "clustering")
        private String clusteringKey;

        public CompoundPK() {
        }

        public CompoundPK(Long partitionKey, String clusteringKey) {
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
