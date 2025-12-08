package aus2_sem2.storage;

import aus2_sem2.model.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Jeden logický blok súboru.
 *
 * Použitie:
 *  - ako obyčajný blok v HeapFile (neutriedený súbor),
 *  - ako blok primárneho alebo overflow súboru v lineárnom hešovaní.
 *
 * Formát na disku:
 *   int validCount;                // počet platných záznamov v bloku
 *   pre každý slot (0..capacity-1):
 *      byte flag;                  // 1 = obsadený, 0 = prázdny
 *      recordSize bajtov;          // obsah záznamu alebo nulové bajty
 */
public class Block<T extends Record> {

    /** Index bloku v súbore (len pre debug; neukladá sa na disk). */
    private final int blockIndex;
    /** Kapacita bloku – max. počet záznamov. */
    private final int capacity;
    /** Veľkosť jedného záznamu v bajtoch. */
    private final int recordSize;
    /** Trieda záznamu. */
    private final Class<T> recordClass;

    /** Počet platných záznamov v tomto bloku. */
    private int validCount;

    @SuppressWarnings("unchecked")
    private final T[] records;

    /**
     * Vytvorí úplne prázdny blok (všetky sloty sú null).
     */
    @SuppressWarnings("unchecked")
    public Block(int blockIndex, int capacity, int recordSize, Class<T> recordClass, boolean empty) {
        this.blockIndex = blockIndex;
        this.capacity = capacity;
        this.recordSize = recordSize;
        this.recordClass = recordClass;
        this.records = (T[]) new Record[capacity];
        this.validCount = 0;

        if (empty) {
            for (int i = 0; i < capacity; i++) {
                records[i] = null;
            }
        }
    }

    /**
     * Konštruktor používaný pri čítaní zo súboru (obsah sa doplní vo fromByteArray()).
     */
    public Block(int blockIndex, int capacity, int recordSize, Class<T> recordClass) {
        this(blockIndex, capacity, recordSize, recordClass, false);
    }

    // --- základné gettre ---

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

    /** Blok je plný, ak nemá voľné sloty. */
    public boolean isFull() {
        return validCount >= capacity;
    }

    /** Blok je prázdny, ak neobsahuje žiadny záznam. */
    public boolean isEmpty() {
        return validCount == 0;
    }

    // --- manipulácia so záznamami ---

    /**
     * Vloží záznam do prvého voľného slotu.
     *
     * @return index slotu alebo -1 ak je blok plný.
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

    /** Vráti záznam v danom slote alebo null. */
    public T getRecord(int slotIndex) {
        if (slotIndex < 0 || slotIndex >= capacity) {
            throw new IndexOutOfBoundsException("slotIndex out of range: " + slotIndex);
        }
        return records[slotIndex];
    }

    /**
     * Zmaže záznam v danom slote.
     * @return true ak tam bol platný záznam a bol zmazaný.
     */
    public boolean delete(int slotIndex) {
        if (slotIndex < 0 || slotIndex >= capacity) {
            throw new IndexOutOfBoundsException("slotIndex out of range: " + slotIndex);
        }
        if (records[slotIndex] != null) {
            records[slotIndex] = null;
            validCount--;
            return true;
        }
        return false;
    }

    /**
     * Sekvenčné vyhľadanie podľa ID (pomocná metóda pre HeapFile / LinHash).
     */
    public T findById(String id) {
        if (id == null) {
            return null;
        }
        for (int i = 0; i < capacity; i++) {
            T r = records[i];
            if (r != null && id.equals(r.getId())) {
                return r;
            }
        }
        return null;
    }

    /**
     * Zmazanie prvého záznamu s daným ID.
     */
    public boolean deleteById(String id) {
        if (id == null) {
            return false;
        }
        for (int i = 0; i < capacity; i++) {
            T r = records[i];
            if (r != null && id.equals(r.getId())) {
                records[i] = null;
                validCount--;
                return true;
            }
        }
        return false;
    }

    // --- serializácia / deserializácia ---

    /**
     * Serializuje blok do poľa bajtov.
     * Diskový formát: int validCount + (byte flag + recordSize bajtov) * capacity.
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
                                        ") != expected recordSize (" + recordSize + ")"
                        );
                    }
                    dos.write(data);
                } else {
                    dos.writeByte(0);
                    dos.write(new byte[recordSize]);
                }
            }

            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Error in Block.toByteArray()", e);
        }
    }

    /**
     * Naplní blok z poľa bajtov v rovnakom formáte, ako vracia toByteArray().
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
            throw new IllegalStateException("Error in Block.fromByteArray()", e);
        }
    }

    private T createEmptyRecord() {
        try {
            return recordClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create record instance " + recordClass.getName(), e);
        }
    }

    /**
     * Debug výpis obsahu bloku (ID záznamov alebo <empty>).
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
