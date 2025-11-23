package aus2_sem2.storage;

import aus2_sem2.model.Record;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Neutriedený heap súbor na disku (blokovo orientované uloženie záznamov).
 */
public class HeapFile<T extends Record> {

    private final String filePath;          // cesta k súboru na disku
    private final Class<T> recordClass;    // typ záznamu (kvôli reflexii)
    private final RandomAccessFile raf;    // nízkoúrovňový prístup k súboru

    private final int recordSize;          // veľkosť jedného záznamu
    private final int recordsPerBlock;     // počet záznamov v jednom bloku
    private int blockSizeBytes;            // veľkosť bloku v bajtoch

    private int blockCount;                // aktuálny počet blokov v súbore

    private final List<Integer> freeBlocks;    // úplne prázdne bloky
    private final List<Integer> partialBlocks; // čiastočne zaplnené bloky

    private int totalValidRecords;         // počet platných záznamov v celom súbore

    public HeapFile(String filePath, int clusterSizeBytes, Class<T> recordClass) {
        try {
            this.filePath = filePath;
            this.recordClass = recordClass;
            this.freeBlocks = new ArrayList<>();
            this.partialBlocks = new ArrayList<>();
            this.totalValidRecords = 0;

            // zistíme veľkosť záznamu z prázdnej inštancie
            T tmp = recordClass.getDeclaredConstructor().newInstance();
            this.recordSize = tmp.getSize();

            if (clusterSizeBytes <= 0) {
                throw new IllegalArgumentException("Cluster size must be positive.");
            }
            if (recordSize <= 0) {
                throw new IllegalStateException("Record size must be positive.");
            }

            // výpočet kapacity: 4 bajty validCount + (flag + data) pre každý záznam
            int tempCapacity = (clusterSizeBytes - 4) / (1 + recordSize);
            if (tempCapacity <= 0) {
                throw new IllegalStateException(
                        "Block too small for even one record. clusterSize=" + clusterSizeBytes +
                        ", recordSize=" + recordSize);
            }

            this.recordsPerBlock = tempCapacity;
            this.blockSizeBytes = 4 + recordsPerBlock * (1 + recordSize);

            // otvorenie / vytvorenie súboru
            this.raf = new RandomAccessFile(filePath, "rw");

            long len = raf.length();
            // zarovnanie dĺžky súboru na násobok veľkosti bloku
            if (len % blockSizeBytes != 0) {
                long newLen = (len / blockSizeBytes) * blockSizeBytes;
                raf.setLength(newLen);
                len = newLen;
            }

            this.blockCount = (int) (len / blockSizeBytes);

            // obnovíme zoznam voľných / čiastočných blokov zo súboru
            rebuildFreeListsFromFileHeader();
        } catch (Exception e) {
            throw new IllegalStateException("Error initializing HeapFile: " + e.getMessage(), e);
        }
    }

    public int getBlockSizeBytes() {
        return blockSizeBytes;
    }

    public int getRecordsPerBlock() {
        return recordsPerBlock;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public int getBlockCount() {
        return blockCount;
    }

    public synchronized int getTotalValidRecords() {
        return totalValidRecords;
    }

    public void close() {
        try {
            raf.close();
        } catch (IOException e) {
            throw new IllegalStateException("Error closing HeapFile", e);
        }
    }

    // skladanie adresy (blok, slot) do jedného long
    public static long makeAddress(int blockIndex, int slotIndex) {
        return (((long) blockIndex) << 32) | (slotIndex & 0xffffffffL);
    }

    public static int getBlockIndexFromAddress(long address) {
        return (int) (address >> 32);
    }

    public static int getSlotIndexFromAddress(long address) {
        return (int) (address & 0xffffffffL);
    }

    // ==== základná INSERT bez kontroly unikátnosti ID ====

    /**
     * Vloží záznam do súboru (bez kontroly duplicitného ID).
     * Vyberá vhodný blok podľa zoznamov partialBlocks/freeBlocks alebo alokuje nový.
     */
    public synchronized long insert(T record) {
        if (record == null) {
            throw new IllegalArgumentException("Inserted record cannot be null.");
        }

        try {
            int targetBlockIndex;

            // preferujeme čiastočne zaplnený blok, potom úplne prázdny
            if (!partialBlocks.isEmpty()) {
                targetBlockIndex = partialBlocks.get(0);
            } else if (!freeBlocks.isEmpty()) {
                targetBlockIndex = freeBlocks.remove(0);
            } else {
                targetBlockIndex = allocateNewBlockIndex();
            }

            Block<T> block;
            // buď blok načítame, alebo vytvoríme nový
            if (targetBlockIndex < blockCount - 1 || targetBlockIndex < existingBlockCountFromLength()) {
                block = readBlock(targetBlockIndex);
            } else {
                block = new Block<>(targetBlockIndex, recordsPerBlock, recordSize, recordClass, true);
            }

            int slot = block.insert(record);
            if (slot < 0) {
                // fallback – ak je blok plný, hľadáme ďalší
                block = findOrCreateBlockForInsert(record);
                targetBlockIndex = block.getBlockIndex();
                slot = block.insert(record);
                if (slot < 0) {
                    throw new IllegalStateException("No free slot even in newly allocated block.");
                }
            }

            writeBlock(block);
            updateListsAfterInsert(block);
            totalValidRecords++;

            return makeAddress(targetBlockIndex, slot);
        } catch (IOException e) {
            throw new IllegalStateException("Error during insert operation", e);
        }
    }

    // ==== INSERT s kontrolou unikátneho ID (vráti -1 ak ID už existuje) ====

    /**
     * Vloží záznam len v prípade, že v súbore ešte neexistuje záznam s daným ID.
     *
     * @return adresa záznamu alebo -1, ak ID už existuje
     */
    public synchronized long insertUnique(T record) {
        if (record == null) {
            throw new IllegalArgumentException("Inserted record cannot be null.");
        }
        String id = record.getId();
        if (id != null && existsId(id)) {
            return -1L;
        }
        return insert(record);
    }

    // ==== ČÍTANIE ====

    /**
     * Vráti záznam na danej adrese (blok, slot) alebo null, ak adresa nie je platná.
     */
    public synchronized T get(long address) {
        int blockIndex = getBlockIndexFromAddress(address);
        int slotIndex = getSlotIndexFromAddress(address);

        if (blockIndex < 0 || blockIndex >= blockCount) {
            return null;
        }
        if (slotIndex < 0 || slotIndex >= recordsPerBlock) {
            return null;
        }

        try {
            Block<T> block = readBlock(blockIndex);
            return block.getRecord(slotIndex);
        } catch (IOException e) {
            throw new IllegalStateException("Error during get operation", e);
        }
    }

    // ==== MAZANIE ====

    /**
     * Zmaže záznam na danej adrese. Pri vyprázdnení posledných blokov
     * sa súbor skráti (shrinkTrailingEmptyBlocks).
     */
    public synchronized boolean delete(long address) {
        int blockIndex = getBlockIndexFromAddress(address);
        int slotIndex = getSlotIndexFromAddress(address);

        if (blockIndex < 0 || blockIndex >= blockCount) {
            return false;
        }
        if (slotIndex < 0 || slotIndex >= recordsPerBlock) {
            return false;
        }

        try {
            Block<T> block = readBlock(blockIndex);
            boolean removed = block.delete(slotIndex);
            if (!removed) {
                return false;
            }

            totalValidRecords--;

            if (block.isEmpty()) {
                // prázdny blok – zapíšeme stav a prípadne skrátime súbor
                writeBlock(block);
                shrinkTrailingEmptyBlocks();
                if (blockIndex < blockCount) {
                    removeFromList(partialBlocks, blockIndex);
                    addToListIfAbsent(freeBlocks, blockIndex);
                }
            } else {
                // blok nie je prázdny – len aktualizujeme zoznamy
                writeBlock(block);
                updateListsAfterDelete(block);
            }

            return true;
        } catch (IOException e) {
            throw new IllegalStateException("Error during delete operation", e);
        }
    }

    // ==== ZISTENIE, ČI EXISTUJE DANÉ ID V SÚBORE ====

    /**
     * Sekvenčne prehľadá všetky bloky a zistí, či existuje záznam s daným ID.
     */
    public synchronized boolean existsId(String id) {
        if (id == null || id.isEmpty()) return false;

        try {
            for (int i = 0; i < blockCount; i++) {
                Block<T> block = readBlock(i);
                for (int slot = 0; slot < recordsPerBlock; slot++) {
                    T rec = block.getRecord(slot);
                    if (rec != null && id.equals(rec.getId())) {
                        return true;
                    }
                }
            }
            return false;
        } catch (IOException e) {
            throw new IllegalStateException("Error during existsId()", e);
        }
    }

    // ==== ZOZNAM VŠETKÝCH ADRIES ====

    /**
     * Vráti zoznam adries všetkých platných záznamov v súbore.
     */
    public synchronized List<Long> getAllAddresses() {
        List<Long> result = new ArrayList<>();
        try {
            for (int i = 0; i < blockCount; i++) {
                Block<T> block = readBlock(i);
                for (int slot = 0; slot < recordsPerBlock; slot++) {
                    T rec = block.getRecord(slot);
                    if (rec != null) {
                        long addr = makeAddress(i, slot);
                        result.add(addr);
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error during getAllAddresses()", e);
        }
        return result;
    }

    // ==== DEBUG DUMP ====

    /**
     * Textový debug výpis celého súboru – základné parametre + obsah každého bloku.
     */
    public synchronized String dumpDebugInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("HeapFile debug dump\n");
        sb.append("filePath: ").append(filePath).append("\n");
        sb.append("blockSizeBytes: ").append(blockSizeBytes).append("\n");
        sb.append("recordSize: ").append(recordSize).append("\n");
        sb.append("recordsPerBlock: ").append(recordsPerBlock).append("\n");
        sb.append("blockCount: ").append(blockCount).append("\n");
        sb.append("totalValidRecords: ").append(totalValidRecords).append("\n");
        sb.append("freeBlocks: ").append(freeBlocks.toString()).append("\n");
        sb.append("partialBlocks: ").append(partialBlocks.toString()).append("\n");
        sb.append("\n");

        try {
            for (int i = 0; i < blockCount; i++) {
                Block<T> block = readBlock(i);
                sb.append(block.debugString()).append("\n");
            }
        } catch (IOException e) {
            sb.append("Error during dump: ").append(e.getMessage()).append("\n");
        }

        return sb.toString();
    }

    // ==== INTERNAL UTILS ====


    /** Zistí počet existujúcich blokov podľa dĺžky súboru. */
    private int existingBlockCountFromLength() throws IOException {
        long len = raf.length();
        return (int) (len / blockSizeBytes);
    }

    /**
     * Načíta z hlavičiek blokov hodnotu validCount a podľa toho doplní
     * freeBlocks a partialBlocks. Zároveň dopočíta totalValidRecords.
     */
    private void rebuildFreeListsFromFileHeader() throws IOException {
        freeBlocks.clear();
        partialBlocks.clear();
        totalValidRecords = 0;

        for (int i = 0; i < blockCount; i++) {
            raf.seek((long) i * blockSizeBytes);
            int validCount;
            try {
                validCount = raf.readInt();
            } catch (IOException e) {
                validCount = 0;
            }

            if (validCount < 0 || validCount > recordsPerBlock) {
                validCount = 0;
            }

            totalValidRecords += validCount;

            if (validCount == 0) {
                freeBlocks.add(i);
            } else if (validCount < recordsPerBlock) {
                partialBlocks.add(i);
            }
        }

        // odstráni prázdne bloky na konci súboru
        shrinkTrailingEmptyBlocks();
    }

    /** Alokuje index pre nový blok (logicky ho pridá na koniec). */
    private int allocateNewBlockIndex() {
        return blockCount++;
    }

    /**
     * Nájde alebo vytvorí blok vhodný na vloženie nového záznamu.
     * Preferuje partialBlocks, potom freeBlocks, inak alokuje nový.
     */
    private Block<T> findOrCreateBlockForInsert(T record) throws IOException {
        if (!partialBlocks.isEmpty()) {
            int bi = partialBlocks.get(0);
            return readBlock(bi);
        }

        if (!freeBlocks.isEmpty()) {
            int bi = freeBlocks.remove(0);
            return readBlock(bi);
        }

        int newIndex = allocateNewBlockIndex();
        return new Block<>(newIndex, recordsPerBlock, recordSize, recordClass, true);
    }

    /**
     * Načíta jeden blok z disku podľa indexu.
     */
    private Block<T> readBlock(int blockIndex) throws IOException {
        if (blockIndex < 0 || blockIndex >= blockCount) {
            throw new IndexOutOfBoundsException("Block index out of range: " + blockIndex);
        }

        byte[] buf = new byte[blockSizeBytes];
        raf.seek((long) blockIndex * blockSizeBytes);
        int read = raf.read(buf);
        if (read < blockSizeBytes) {
            throw new IOException("Cannot read full block " + blockIndex + ", read=" + read);
        }

        Block<T> block = new Block<>(blockIndex, recordsPerBlock, recordSize, recordClass);
        block.fromByteArray(buf);
        return block;
    }

    /**
     * Zapíše jeden blok na disk na jeho pozíciu.
     */
    private void writeBlock(Block<T> block) throws IOException {
        byte[] data = block.toByteArray();
        if (data.length != blockSizeBytes) {
            throw new IllegalStateException(
                    "Block serialized size (" + data.length + ") != blockSizeBytes (" + blockSizeBytes + ")");
        }
        int idx = block.getBlockIndex();
        raf.seek((long) idx * blockSizeBytes);
        raf.write(data);
    }

    /**
     * Orezáva (skracuje) súbor o posledné prázdne bloky.
     * Posledný blok nikdy nesmie zostať prázdny kvôli zadaniu.
     */
    private void shrinkTrailingEmptyBlocks() throws IOException {
        while (blockCount > 0) {
            int lastIndex = blockCount - 1;
            raf.seek((long) lastIndex * blockSizeBytes);
            int validCount;
            try {
                validCount = raf.readInt();
            } catch (IOException e) {
                break;
            }

            if (validCount != 0) {
                break;
            }

            blockCount--;
            long newLength = (long) blockCount * blockSizeBytes;
            raf.setLength(newLength);

            removeFromList(freeBlocks, lastIndex);
            removeFromList(partialBlocks, lastIndex);
        }
    }

    /**
     * Aktualizácia zoznamov freeBlocks/partialBlocks po vkladaní.
     */
    private void updateListsAfterInsert(Block<T> block) {
        int idx = block.getBlockIndex();
        if (block.isFull()) {
            removeFromList(partialBlocks, idx);
        } else if (block.getValidCount() > 0) {
            removeFromList(freeBlocks, idx);
            addToListIfAbsent(partialBlocks, idx);
        } else {
            removeFromList(partialBlocks, idx);
            addToListIfAbsent(freeBlocks, idx);
        }
    }

    /**
     * Aktualizácia zoznamov freeBlocks/partialBlocks po mazaní.
     */
    private void updateListsAfterDelete(Block<T> block) {
        int idx = block.getBlockIndex();
        if (block.isEmpty()) {
            removeFromList(partialBlocks, idx);
            addToListIfAbsent(freeBlocks, idx);
        } else if (block.isFull()) {
            removeFromList(partialBlocks, idx);
            removeFromList(freeBlocks, idx);
        } else {
            removeFromList(freeBlocks, idx);
            addToListIfAbsent(partialBlocks, idx);
        }
    }

    /** Odstráni hodnotu zo zoznamu, ak tam je. */
    private void removeFromList(List<Integer> list, int value) {
        list.remove(Integer.valueOf(value));
    }

    /** Pridá hodnotu do zoznamu len vtedy, ak tam ešte nie je. */
    private void addToListIfAbsent(List<Integer> list, int value) {
        if (!list.contains(value)) {
            list.add(value);
        }
    }
}
