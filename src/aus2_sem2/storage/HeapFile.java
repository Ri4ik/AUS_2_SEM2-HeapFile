package aus2_sem2.storage;

import aus2_sem2.model.Record;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Реализация неупорядоченного файла (heap file) на диске.
 *
 * Использует фиксированные блоки одинакового размера.
 * Адрес записи кодируется как long: старшие 32 бита = индекс блока,
 * младшие 32 бита = индекс слота внутри блока.
 *
 * Формат блока описан в Block<T>.
 */
public class HeapFile<T extends Record> {

    private final String filePath;
    private final Class<T> recordClass;
    private final RandomAccessFile raf;

    private final int recordSize;
    private final int recordsPerBlock;
    private int blockSizeBytes;            // фактический размер блока в байтах

    private int blockCount;                // количество блоков в файле

    private final List<Integer> freeBlocks;     // полностью пустые блоки (validCount = 0, не в конце файла)
    private final List<Integer> partialBlocks;  // блоки с 0 < validCount < capacity

    // новое: общее количество валидных записей во всём файле
    private int totalValidRecords;

    /**
     * Создаёт/открывает heap-файл.
     *
     * @param filePath путь к файлу
     * @param clusterSizeBytes желаемый размер кластера/блока (например 4096)
     * @param recordClass класс записи (например PatientRecord.class)
     */
    public HeapFile(String filePath, int clusterSizeBytes, Class<T> recordClass) {
        try {
            this.filePath = filePath;
            this.recordClass = recordClass;
            this.freeBlocks = new ArrayList<>();
            this.partialBlocks = new ArrayList<>();
            this.totalValidRecords = 0;

            T tmp = recordClass.getDeclaredConstructor().newInstance();
            this.recordSize = tmp.getSize();

            if (clusterSizeBytes <= 0) {
                throw new IllegalArgumentException("Cluster size must be positive.");
            }
            if (recordSize <= 0) {
                throw new IllegalStateException("Record size must be positive.");
            }

            int tempCapacity = (clusterSizeBytes - 4) / (1 + recordSize);
            if (tempCapacity <= 0) {
                throw new IllegalStateException(
                        "Block too small for even one record. clusterSize=" + clusterSizeBytes +
                        ", recordSize=" + recordSize);
            }

            this.recordsPerBlock = tempCapacity;
            this.blockSizeBytes = 4 + recordsPerBlock * (1 + recordSize);

            this.raf = new RandomAccessFile(filePath, "rw");

            long len = raf.length();
            if (len % blockSizeBytes != 0) {
                long newLen = (len / blockSizeBytes) * blockSizeBytes;
                raf.setLength(newLen);
                len = newLen;
            }

            this.blockCount = (int) (len / blockSizeBytes);

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

    // новое: получить общее количество валидных записей
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

    public static long makeAddress(int blockIndex, int slotIndex) {
        return (((long) blockIndex) << 32) | (slotIndex & 0xffffffffL);
    }

    public static int getBlockIndexFromAddress(long address) {
        return (int) (address >> 32);
    }

    public static int getSlotIndexFromAddress(long address) {
        return (int) (address & 0xffffffffL);
    }

    /**
     * Вставка одной записи. Возвращает адрес (blockIndex/slotIndex).
     */
    public synchronized long insert(T record) {
        if (record == null) {
            throw new IllegalArgumentException("Inserted record cannot be null.");
        }

        try {
            int targetBlockIndex;

            if (!partialBlocks.isEmpty()) {
                targetBlockIndex = partialBlocks.get(0);
            } else if (!freeBlocks.isEmpty()) {
                targetBlockIndex = freeBlocks.remove(0);
            } else {
                targetBlockIndex = allocateNewBlockIndex();
            }

            Block<T> block;
            if (targetBlockIndex < blockCount - 1 || targetBlockIndex < existingBlockCountFromLength()) {
                block = readBlock(targetBlockIndex);
            } else {
                block = new Block<>(targetBlockIndex, recordsPerBlock, recordSize, recordClass, true);
            }

            int slot = block.insert(record);
            if (slot < 0) {
                block = findOrCreateBlockForInsert(record);
                targetBlockIndex = block.getBlockIndex();
                slot = block.insert(record);
                if (slot < 0) {
                    throw new IllegalStateException("No free slot even in newly allocated block.");
                }
            }

            writeBlock(block);

            updateListsAfterInsert(block);

            // учёт общего количества записей
            totalValidRecords++;

            return makeAddress(targetBlockIndex, slot);
        } catch (IOException e) {
            throw new IllegalStateException("Error during insert operation", e);
        }
    }

    /**
     * Чтение записи по адресу.
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

    /**
     * Удаление записи по адресу.
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

            // учёт общего количества записей
            totalValidRecords--;

           if (block.isEmpty()) {
    // сначала записать пустой блок на диск, чтобы его validCount стал 0 в файле
    writeBlock(block);

    // потом обрезать хвост — эта функция сама посмотрит на все последние блоки
    shrinkTrailingEmptyBlocks();

    // если после обрезки наш блок ещё существует (он не был в конце),
    // тогда он должен быть в списке свободных
    if (blockIndex < blockCount) {
        removeFromList(partialBlocks, blockIndex);
        addToListIfAbsent(freeBlocks, blockIndex);
    }
    } else {
        writeBlock(block);
        updateListsAfterDelete(block);
    }

            return true;
        } catch (IOException e) {
            throw new IllegalStateException("Error during delete operation", e);
        }
    }

    /**
     * Вернуть список адресов всех валидных записей (для случайного удаления и т.п.).
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

    /**
     * Для отладочного вывода.
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

    private int existingBlockCountFromLength() throws IOException {
        long len = raf.length();
        return (int) (len / blockSizeBytes);
    }

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
        // а теперь уже чисто убираем пустые блоки с конца
        shrinkTrailingEmptyBlocks();
    }


    private int allocateNewBlockIndex() {
        return blockCount++;
    }

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

//    private void shrinkFileFromEnd(int lastIndex) throws IOException {
//        if (lastIndex != blockCount - 1) {
//            return;
//        }
//
//        blockCount--;
//        long newLength = (long) blockCount * blockSizeBytes;
//        raf.setLength(newLength);
//
//        removeFromList(freeBlocks, lastIndex);
//        removeFromList(partialBlocks, lastIndex);
//    }
    private void shrinkTrailingEmptyBlocks() throws IOException {
    while (blockCount > 0) {
        int lastIndex = blockCount - 1;

        // читаем validCount последнего блока
        raf.seek((long) lastIndex * blockSizeBytes);
        int validCount;
        try {
            validCount = raf.readInt();
        } catch (IOException e) {
            // если не смогли прочитать, выходим, чтобы не повредить файл
            break;
        }

        // если последний блок НЕ пустой — дальше обрезать нельзя
        if (validCount != 0) {
            break;
        }

        // иначе удаляем этот блок с конца
        blockCount--;
        long newLength = (long) blockCount * blockSizeBytes;
        raf.setLength(newLength);

        // чистим списки freeBlocks/partialBlocks от этого индекса
        removeFromList(freeBlocks, lastIndex);
        removeFromList(partialBlocks, lastIndex);
    }
}


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

    private void removeFromList(List<Integer> list, int value) {
        list.remove(Integer.valueOf(value));
    }

    private void addToListIfAbsent(List<Integer> list, int value) {
        if (!list.contains(value)) {
            list.add(value);
        }
    }
}
