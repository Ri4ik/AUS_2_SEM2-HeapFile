package aus2_sem2.model;

public interface Record {

    int getSize();

    byte[] toByteArray();

    void fromByteArray(byte[] data);

    boolean equals(Record other);
}
