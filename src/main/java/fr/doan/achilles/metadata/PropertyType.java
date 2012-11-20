package fr.doan.achilles.metadata;

public enum PropertyType {
    META(0), START_EAGER(1), SIMPLE(2), LIST(3), SET(4), MAP(5), END_EAGER(6), ITERATOR(7);

    private int flag;

    PropertyType(int flag) {
        this.flag = flag;
    }

    public byte[] flag() {
        return new byte[] { (byte) flag };
    }
}
