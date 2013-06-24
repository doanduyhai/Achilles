package info.archinnov.achilles.type;

/**
 * BoundingMode
 * 
 * @author DuyHai DOAN
 * 
 */
public enum BoundingMode {
    INCLUSIVE_BOUNDS(true, true),
    EXCLUSIVE_BOUNDS(false, false),
    INCLUSIVE_START_BOUND_ONLY(true, false),
    INCLUSIVE_END_BOUND_ONLY(false, true);

    private boolean inclusiveStart;
    private boolean inclusiveEnd;

    private BoundingMode(boolean inclusiveStart, boolean inclusiveEnd) {
        this.inclusiveStart = inclusiveStart;
        this.inclusiveEnd = inclusiveEnd;
    }

    public boolean isInclusiveStart()
    {
        return inclusiveStart;
    }

    public boolean isInclusiveEnd()
    {
        return inclusiveEnd;
    }
}
