package fr.doan.achilles.metadata;

public enum PropertyType {
    META(0), SIMPLE(1), LIST(2), SET(3), MAP(4), ITERATOR(5);

    private int flag;

    PropertyType(int flag) {
        this.flag = flag;
    }

    public byte[] flag() {
        return new byte[] { (byte) flag };
    }
}
