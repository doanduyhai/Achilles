package info.archinnov.achilles.type;

/**
 * OrderingMode
 * 
 * @author DuyHai DOAN
 * 
 */
public enum OrderingMode {
    DESCENDING(true, "DESC"),
    ASCENDING(false, "ASC");

    private boolean reverse;
    private String string;

    private OrderingMode(boolean equivalent, String string) {
        this.reverse = equivalent;
        this.string = string;
    }

    public boolean isReverse()
    {
        return reverse;
    }

    public String asString()
    {
        return string;
    }
}
