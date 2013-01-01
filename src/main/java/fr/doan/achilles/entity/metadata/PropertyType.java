package fr.doan.achilles.entity.metadata;

public enum PropertyType {
    META(0, false), //
    START_EAGER(10, false), //
    SIMPLE(11, false), //
    LIST(12, true), // 
    SET(13, true), // 
    MAP(14, true), // 
    END_EAGER(20, false), //
    LAZY_SIMPLE(21, true), //
    LAZY_LIST(22, true), //
    LAZY_SET(23, true), //
    LAZY_MAP(24, true), //
    WIDE_MAP(30, true), //
    EXTERNAL_WIDE_MAP(40, true), //
    JOIN_SIMPLE(50, false), //
    JOIN_WIDE_MAP(60, true);

    private int flag;
    private boolean multiValue;

    PropertyType(int flag, boolean multiValue) {
        this.flag = flag;
        this.multiValue = multiValue;
    }

    public byte[] flag() {
        return new byte[] { (byte) flag };
    }

    public boolean isMultiValue() {
        return multiValue;
    }
}
