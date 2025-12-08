package aus2_sem2.storage;

import aus2_sem2.model.Record;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * HeapFile – blokovaná organizácia záznamov typu T nad jedným súborom.
 *
 * Formát bloku (Block<T>):
 *   int validCount;               // 4 bajty
 *   pre každý slot:
 *      byte flag;                 // 1 = obsadený, 0 = prázdny
 *      recordSize bajtov          // obsah záznamu alebo nulové bajty
 *
 * API:
 *  - používa ho HeapFileTester (insert/get/delete/existsId/getAllAddresses/getTotalValidRecords)
 *  - používa ho LinHashFile (readBlock/writeBlock/allocateEmptyBlockOrReuse/shrinkEmptyTailBlocks/getBlockCount/getRecordsPerBlock)
 */
public class HeapFile<T extends Record> {

    /** Súbor na disku. */
    private final RandomAccessFile raf;

    /** Veľkosť jedného bloku na disku v bajtoch. */
    private final int blockSizeBytes;

    /** Veľkosť jedného záznamu v bajtoch (Record.getSize()). */
    private final int recordSize;

    /** Počet záznamov v jednom bloku. */
    private final int recordsPerBlock;

    /** Trieda záznamu (na reflexiu). */
    private final Class<T> recordClass;

    /** Počet blokov v súbore (0..blockCount-1). */
    private int blockCount;

    /** Indexy úplne prázdnych blokov. (Používa sa iba pre „obyčajný“ insert, nie pre LinHash allocateEmptyBlockOrReuse) */
    private final List<Integer> freeBlocks = new ArrayList<>();

    /** Indexy čiastočne zaplnených blokov. */
    private final List<Integer> partialBlocks = new ArrayList<>();

    /** Celkový počet platných záznamov v súbore. */
    private int totalValidRecords = 0;

    /**
     * Vytvorí alebo otvorí HeapFile nad daným súborom.
     *
     * @param filePath cesta k súboru
     * @param clusterSizeBytes veľkosť bloku požadovaná zvonka (napr. 256)
     * @param recordClass trieda záznamov (napr. PatientRecord.class)
     */
    public HeapFile(String filePath, int clusterSizeBytes, Class<T> recordClass) {
        try {
            this.recordClass = recordClass;

            // zistíme recordSize z prázdneho objektu T
            T tmp = recordClass.getDeclaredConstructor().newInstance();
            this.recordSize = tmp.getSize();
            if (recordSize <= 0) {
                throw new IllegalStateException("Record size must be positive.");
            }

            // skutočný header v Block<T>:
            //   int validCount => 4 bajty
            int headerSize = Integer.BYTES; // 4

            // koľko slotov vieme dať do jedného bloku
            int tempCapacity = (clusterSizeBytes - headerSize) / (1 + recordSize);
            if (tempCapacity <= 0) {
                throw new IllegalStateException(
                        "Cluster too small for one record. clusterSize=" + clusterSizeBytes +
                                ", recordSize=" + recordSize);
            }

            this.recordsPerBlock = tempCapacity;
            this.blockSizeBytes = headerSize + recordsPerBlock * (1 + recordSize);

            File f = new File(filePath);
            this.raf = new RandomAccessFile(f, "rw");

            long len = raf.length();
            if (len % blockSizeBytes != 0) {
                throw new IllegalStateException(
                        "File length is not a multiple of blockSizeBytes. len=" + len +
                                ", blockSizeBytes=" + blockSizeBytes);
            }
            this.blockCount = (int) (len / blockSizeBytes);

            // ak súbor ešte nemá žiadny blok, začíname „prázdny“
            if (blockCount == 0) {
                // prvý blok vznikne pri prvom insert-e
            } else {
                // obnovíme free/partial lists a spočítame totalValidRecords
                rebuildFreeListsInternal();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Error initializing HeapFile: " + e.getMessage(), e);
        }
    }

    // =========================================================
    //  Základné gettre pre LinHash
    // =========================================================

    /** Počet blokov v súbore. */
    public synchronized int getBlockCount() {
        return blockCount;
    }

    /** Počet záznamov v jednom bloku. */
    public int getRecordsPerBlock() {
        return recordsPerBlock;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public Class<T> getRecordClass() {
        return recordClass;
    }

    // =========================================================
    //  Adresácia: (blockIndex, slotIndex) -> long address
    // =========================================================

    private long makeAddress(int blockIndex, int slotIndex) {
        return (((long) blockIndex) << 32) | (slotIndex & 0xffffffffL);
    }

    private int extractBlockIndex(long address) {
        return (int) (address >>> 32);
    }

    private int extractSlotIndex(long address) {
        return (int) address;
    }

    // =========================================================
    //  Nízkoúrovňové I/O blokov (internal)
    // =========================================================

    private long blockOffset(int blockIndex) {
        return (long) blockIndex * blockSizeBytes;
    }

    private Block<T> readBlockInternal(int blockIndex) throws IOException {
        if (blockIndex < 0 || blockIndex >= blockCount) {
            throw new IndexOutOfBoundsException("Block index out of range: " + blockIndex);
        }
        byte[] buf = new byte[blockSizeBytes];
        raf.seek(blockOffset(blockIndex));
        raf.readFully(buf);

        Block<T> block = new Block<>(blockIndex, recordsPerBlock, recordSize, recordClass);
        block.fromByteArray(buf);
        return block;
    }

    private void writeBlockInternal(Block<T> block) throws IOException {
        byte[] data = block.toByteArray();
        if (data.length != blockSizeBytes) {
            throw new IllegalStateException(
                    "Block serialized size (" + data.length + ") != blockSizeBytes (" + blockSizeBytes + ")");
        }
        int idx = block.getBlockIndex();
        raf.seek(blockOffset(idx));
        raf.write(data);
    }

    /**
     * Vytvorí nový prázdny blok na danom indexe.
     */
    private Block<T> createEmptyBlock(int blockIndex) {
        // initializeEmpty = true -> všetky sloty = null, validCount = 0
        return new Block<>(blockIndex, recordsPerBlock, recordSize, recordClass, true);
    }

    /**
     * Pridá nový prázdny blok na koniec súboru.
     */
    private int appendEmptyBlockInternal() throws IOException {
        int newIndex = blockCount;
        Block<T> block = createEmptyBlock(newIndex);
        writeBlockInternal(block);
        blockCount++;
        freeBlocks.add(newIndex);
        return newIndex;
    }

    /**
     * Znovu vypočíta freeBlocks, partialBlocks a totalValidRecords
     * z aktuálneho obsahu súboru.
     */
    private void rebuildFreeListsInternal() throws IOException {
        freeBlocks.clear();
        partialBlocks.clear();
        totalValidRecords = 0;

        for (int i = 0; i < blockCount; i++) {
            Block<T> block = readBlockInternal(i);
            int vc = block.getValidCount();
            totalValidRecords += vc;

            if (block.isEmpty()) {
                freeBlocks.add(i);
            } else if (!block.isFull()) {
                partialBlocks.add(i);
            }
        }
    }

    /**
     * Aktualizuje zoznamy freeBlocks / partialBlocks pre daný blockIndex.
     */
    private void updateBlockStateLists(int blockIndex, Block<T> block) {
        freeBlocks.remove((Integer) blockIndex);
        partialBlocks.remove((Integer) blockIndex);

        if (block.isEmpty()) {
            freeBlocks.add(blockIndex);
        } else if (!block.isFull()) {
            partialBlocks.add(blockIndex);
        }
    }

    // =========================================================
    //  Public blokové metódy pre LinHashFile
    // =========================================================

    /** Pre LinHash – prečíta blok podľa indexu. */
    public synchronized Block<T> readBlock(int blockIndex) throws IOException {
        return readBlockInternal(blockIndex);
    }

    /** Pre LinHash – zapíše blok na daný index. */
    public synchronized void writeBlock(int blockIndex, Block<T> block) throws IOException {
        if (blockIndex != block.getBlockIndex()) {
            throw new IllegalArgumentException("blockIndex != block.getBlockIndex()");
        }
        writeBlockInternal(block);
        updateBlockStateLists(blockIndex, block);
    }

    /**
     * Pre LinHash: vráti VŽDY fyzicky nový prázdny blok na konci súboru.
     *
     * DÔLEŽITÉ:
     *  - Nevyužívame freeBlocks, aby sa nestalo, že primárny blok nejakej skupiny
     *    (dočasne prázdny) bude znovu pridelený ako nový blok inej skupine.
     *  - Tým garantujeme, že každá skupina v LinHashFile má vlastný fyzický blok
     *    a nikdy nedôjde k zdieľaniu jedného bloku viacerými skupinami.
     */
    public synchronized int allocateEmptyBlockOrReuse() throws IOException {
        return appendEmptyBlockInternal();
    }

    /** Pre LinHash – skráti súbor o súvislý úsek prázdnych blokov na konci. */
    public synchronized void shrinkEmptyTailBlocks() throws IOException {
        shrinkTrailingEmptyBlocksInternal();
    }

    // =========================================================
    //  Verejné API – to, čo používa HeapFileTester
    // =========================================================

    /**
     * Vloží záznam a vráti logickú adresu (blockIndex, slotIndex) zakódovanú v long.
     */
    public synchronized long insert(T record) {
        if (record == null) {
            throw new IllegalArgumentException("Inserted record cannot be null.");
        }

        try {
            int blockIndex;

            // pre obyčajný HeapFile insert sa snažíme využiť partial/free bloky
            if (!partialBlocks.isEmpty()) {
                blockIndex = partialBlocks.get(0);
            } else if (!freeBlocks.isEmpty()) {
                blockIndex = freeBlocks.get(0);
            } else {
                blockIndex = appendEmptyBlockInternal();
            }

            Block<T> block = readBlockInternal(blockIndex);
            int slot = block.insert(record);
            if (slot < 0) {
                // bezpečnostná poistka – nemalo by sa stať
                blockIndex = appendEmptyBlockInternal();
                block = readBlockInternal(blockIndex);
                slot = block.insert(record);
                if (slot < 0) {
                    throw new IllegalStateException("Cannot insert record even into new block.");
                }
            }

            writeBlockInternal(block);
            updateBlockStateLists(blockIndex, block);
            totalValidRecords++;

            return makeAddress(blockIndex, slot);
        } catch (IOException e) {
            throw new IllegalStateException("Error during HeapFile.insert()", e);
        }
    }

    /**
     * Získa záznam podľa adresy (blockIndex, slotIndex).
     *
     * @return záznam alebo null, ak je slot prázdny alebo adresa mimo rozsahu.
     */
    public synchronized T get(long address) {
        int blockIndex = extractBlockIndex(address);
        int slotIndex = extractSlotIndex(address);

        try {
            if (blockIndex < 0 || blockIndex >= blockCount) {
                return null;
            }
            if (slotIndex < 0 || slotIndex >= recordsPerBlock) {
                return null;
            }

            Block<T> block = readBlockInternal(blockIndex);
            return block.getRecord(slotIndex);
        } catch (IOException e) {
            throw new IllegalStateException("Error during HeapFile.get()", e);
        }
    }

    /**
     * Zmaže záznam na danej adrese.
     *
     * @return true, ak bol záznam prítomný a zmazaný.
     */
    public synchronized boolean delete(long address) {
        int blockIndex = extractBlockIndex(address);
        int slotIndex = extractSlotIndex(address);

        try {
            if (blockIndex < 0 || blockIndex >= blockCount) {
                return false;
            }
            if (slotIndex < 0 || slotIndex >= recordsPerBlock) {
                return false;
            }

            Block<T> block = readBlockInternal(blockIndex);
            boolean removed = block.delete(slotIndex);
            if (!removed) {
                return false;
            }

            writeBlockInternal(block);
            updateBlockStateLists(blockIndex, block);
            totalValidRecords--;

            // odrežeme prípadné prázdne bloky na konci
            shrinkTrailingEmptyBlocksInternal();

            return true;
        } catch (IOException e) {
            throw new IllegalStateException("Error during HeapFile.delete()", e);
        }
    }

    /**
     * Skontroluje, či existuje záznam s daným ID kdekoľvek v HeapFile.
     */
    public synchronized boolean existsId(String id) {
        if (id == null || id.isEmpty()) {
            return false;
        }
        try {
            for (int i = 0; i < blockCount; i++) {
                Block<T> block = readBlockInternal(i);
                for (int s = 0; s < recordsPerBlock; s++) {
                    T rec = block.getRecord(s);
                    if (rec != null && id.equals(rec.getId())) {
                        return true;
                    }
                }
            }
            return false;
        } catch (IOException e) {
            throw new IllegalStateException("Error during HeapFile.existsId()", e);
        }
    }

    /**
     * Vráti adresy všetkých platných záznamov (pre HeapFileTester).
     */
    public synchronized List<Long> getAllAddresses() {
        List<Long> result = new ArrayList<>();
        try {
            for (int i = 0; i < blockCount; i++) {
                Block<T> block = readBlockInternal(i);
                for (int s = 0; s < recordsPerBlock; s++) {
                    T rec = block.getRecord(s);
                    if (rec != null) {
                        result.add(makeAddress(i, s));
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error during HeapFile.getAllAddresses()", e);
        }
        return result;
    }

    /**
     * @return celkový počet platných záznamov v súbore.
     */
    public synchronized int getTotalValidRecords() {
        return totalValidRecords;
    }

    /**
     * Bezpečné zatvorenie súboru.
     */
    public synchronized void close() {
        try {
            raf.close();
        } catch (IOException e) {
            throw new IllegalStateException("Error closing HeapFile", e);
        }
    }

    // =========================================================
    //  Orezanie prázdnych blokov na konci – jedna setLength()
    // =========================================================

    /**
     * Orezáva súbor o posledné prázdne bloky (ak existujú) jedným volaním setLength().
     */
    private void shrinkTrailingEmptyBlocksInternal() throws IOException {
        if (blockCount == 0) {
            return;
        }

        int lastNonEmpty = -1;

        // od konca hľadáme posledný NEprázdny blok
        for (int i = blockCount - 1; i >= 0; i--) {
            Block<T> block = readBlockInternal(i);
            if (!block.isEmpty()) {
                lastNonEmpty = i;
                break;
            }
        }

        // ak už posledný blok nie je prázdny – nič nerežeme
        if (lastNonEmpty == blockCount - 1) {
            return;
        }

        // ak sú všetky bloky prázdne, necháme aspoň jeden
        int newBlockCount = (lastNonEmpty == -1) ? 1 : (lastNonEmpty + 1);

        // ak sa počet blokov nemení – koniec
        if (newBlockCount == blockCount) {
            return;
        }

        long newLength = (long) newBlockCount * blockSizeBytes;
        raf.setLength(newLength);

        final int cutoff = newBlockCount;

        // odstránime indexy >= cutoff z free/partial
        freeBlocks.removeIf(idx -> idx >= cutoff);
        partialBlocks.removeIf(idx -> idx >= cutoff);

        blockCount = newBlockCount;
    }

    // =========================================================
    //  Voliteľné debug metódy (môžeš použiť v GUI/konzole)
    // =========================================================

    /**
     * Jednoduchý textový výpis obsahu HeapFile.
     */
    public synchronized String dumpDebugInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("HeapFile dump:\n");
        sb.append("  blockSizeBytes=").append(blockSizeBytes).append("\n");
        sb.append("  recordSize=").append(recordSize).append("\n");
        sb.append("  recordsPerBlock=").append(recordsPerBlock).append("\n");
        sb.append("  blockCount=").append(blockCount).append("\n");
        sb.append("  totalValidRecords=").append(totalValidRecords).append("\n");
        sb.append("  freeBlocks=").append(freeBlocks).append("\n");
        sb.append("  partialBlocks=").append(partialBlocks).append("\n\n");

        try {
            for (int i = 0; i < blockCount; i++) {
                Block<T> block = readBlockInternal(i);
                sb.append("Block #").append(i)
                        .append(" [validCount=")
                        .append(block.getValidCount())
                        .append("]\n");
                for (int s = 0; s < recordsPerBlock; s++) {
                    T rec = block.getRecord(s);
                    sb.append("  slot ").append(s).append(": ");
                    if (rec != null) {
                        sb.append(rec.toString());
                    } else {
                        sb.append("<empty>");
                    }
                    sb.append("\n");
                }
                sb.append("\n");
            }
        } catch (IOException e) {
            sb.append("Error during dump: ").append(e.getMessage()).append("\n");
        }

        return sb.toString();
    }
}
