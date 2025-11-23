package aus2_sem2.storage;

import aus2_sem2.model.Record;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Jeden logický blok heap súboru na disku.
 * Uchováva pevný počet záznamov typu T v pamäti aj v serializovanej forme.
 */
public class Block<T extends Record> {

    private final int blockIndex;   // index bloku v súbore
    private final int capacity;     // max. počet záznamov v bloku
    private final int recordSize;   // veľkosť jedného záznamu v bajtoch
    private final Class<T> recordClass; // typ záznamu kvôli vytváraniu inštancií

    private int validCount;         // počet platných (ne-null) záznamov
    @SuppressWarnings("unchecked")
    private final T[] records;      // pole záznamov v danom bloku

    /**
     * Konštruktor pre blok s možnosťou inicializácie ako „prázdny“.
     */
    public Block(int blockIndex, int capacity, int recordSize, Class<T> recordClass, boolean initializeEmpty) {
        this.blockIndex = blockIndex;
        this.capacity = capacity;
        this.recordSize = recordSize;
        this.recordClass = recordClass;
        this.records = (T[]) new Record[capacity];

        if (initializeEmpty) {
            this.validCount = 0;
            for (int i = 0; i < capacity; i++) {
                this.records[i] = null;
            }
        }
    }

    /**
     * Konštruktor používaný pri načítaní bloku zo súboru.
     */
    @SuppressWarnings("unchecked")
    public Block(int blockIndex, int capacity, int recordSize, Class<T> recordClass) {
        this.blockIndex = blockIndex;
        this.capacity = capacity;
        this.recordSize = recordSize;
        this.recordClass = recordClass;
        this.records = (T[]) new Record[capacity];
        this.validCount = 0;
    }

    public int getBlockIndex() {
        return blockIndex;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public int getValidCount() {
        return validCount;
    }

    /** Blok je plný, ak nemá žiadny voľný slot. */
    public boolean isFull() {
        return validCount >= capacity;
    }

    /** Blok je prázdny, ak neobsahuje žiadny záznam. */
    public boolean isEmpty() {
        return validCount == 0;
    }

    /**
     * Vloží záznam do prvého voľného slotu.
     *
     * @return index slotu alebo -1, ak je blok plný
     */
    public int insert(T rec) {
        if (rec == null) {
            throw new IllegalArgumentException("Inserted record cannot be null.");
        }
        if (isFull()) {
            return -1;
        }
        for (int i = 0; i < capacity; i++) {
            if (records[i] == null) {
                records[i] = rec;
                validCount++;
                return i;
            }
        }
        return -1;
    }

    /** Vráti záznam na danom indexe v bloku. */
    public T getRecord(int index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("Record index out of range: " + index);
        }
        return records[index];
    }

    /**
     * Zmaže záznam na danom indexe (nastaví na null).
     *
     * @return true, ak bol záznam reálne prítomný a zmazaný
     */
    public boolean delete(int index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("Record index out of range: " + index);
        }
        if (records[index] != null) {
            records[index] = null;
            validCount--;
            return true;
        }
        return false;
    }

    /**
     * Serializuje celý blok do poľa bajtov.
     * Formát:
     * <pre>
     *   int validCount;
     *   pre každý slot:
     *     byte flag (1 = obsadený, 0 = prázdny)
     *     recordSize bajtov (dáta záznamu alebo prázdne pole)
     * </pre>
     */
    public byte[] toByteArray() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeInt(validCount);

            for (int i = 0; i < capacity; i++) {
                T rec = records[i];
                if (rec != null) {
                    dos.writeByte(1);
                    byte[] data = rec.toByteArray();
                    if (data.length != recordSize) {
                        throw new IllegalStateException(
                                "Record serialized size (" + data.length +
                                ") != expected recordSize (" + recordSize + ")");
                    }
                    dos.write(data);
                } else {
                    dos.writeByte(0);
                    byte[] empty = new byte[recordSize];
                    dos.write(empty);
                }
            }

            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Error during Block.toByteArray()", e);
        }
    }

    /**
     * Naplní blok z dodaného poľa bajtov v rovnakom formáte, ako vytvára toByteArray().
     */
    public void fromByteArray(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            this.validCount = dis.readInt();

            for (int i = 0; i < capacity; i++) {
                byte flag = dis.readByte();
                byte[] recBytes = new byte[recordSize];
                dis.readFully(recBytes);

                if (flag == 1) {
                    T rec = createEmptyRecord();
                    rec.fromByteArray(recBytes);
                    records[i] = rec;
                } else {
                    records[i] = null;
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error during Block.fromByteArray()", e);
        }
    }

    /**
     * Vytvorí „prázdnu“ inštanciu záznamu typu T pomocou reflexie.
     * Používa sa pri deserializácii z bajtového poľa.
     */
    private T createEmptyRecord() {
        try {
            return recordClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create instance of record class " + recordClass.getName(), e);
        }
    }

    /**
     * Textový debug výpis obsahu bloku (index, validCount a všetky sloty).
     */
    public String debugString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Block #").append(blockIndex)
          .append(" [validCount=").append(validCount)
          .append(", capacity=").append(capacity)
          .append("]\n");
        for (int i = 0; i < capacity; i++) {
            sb.append("  slot ").append(i).append(": ");
            if (records[i] != null) {
                sb.append(records[i].toString());
            } else {
                sb.append("<empty>");
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
