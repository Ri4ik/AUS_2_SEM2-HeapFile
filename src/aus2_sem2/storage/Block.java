package aus2_sem2.storage;

import aus2_sem2.model.Record;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Один блок heap-файла на диске.
 * Хранит фиксированное количество записей T фиксированного размера.
 * Формат блока в байтах:
 *
 * [int validCount]
 * затем для каждого слота:
 *   [byte flagUsed] (0 = пустой, 1 = занято)
 *   [recordSize байт данных записи (или нули, если flagUsed == 0)]
 */
public class Block<T extends Record> {

    private final int blockIndex;     // индекс блока в файле (0,1,2,...)
    private final int capacity;       // сколько записей помещается в блок
    private final int recordSize;     // размер одной записи в байтах
    private final Class<T> recordClass;

    private int validCount;           // количество реально занятых записей
    @SuppressWarnings("unchecked")
    private final T[] records;        // массив записей (null = свободный слот)

    /**
     * Конструктор для создания нового (пустого) блока.
     *
     * @param blockIndex индекс блока в файле
     * @param capacity   количество записей в блоке
     * @param recordSize размер одной записи в байтах
     * @param recordClass класс реализации Record (например PatientRecord.class)
     * @param initializeEmpty если true - создаётся полностью пустой блок
     */
    @SuppressWarnings("unchecked")
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
     * Конструктор для дальнейшего заполнения из fromByteArray().
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

    public boolean isFull() {
        return validCount >= capacity;
    }

    public boolean isEmpty() {
        return validCount == 0;
    }

    /**
     * Вставка записи в первый свободный слот.
     * @param rec запись
     * @return индекс слота [0..capacity-1], либо -1 если нет места
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

    /**
     * Получить запись по индексу слота.
     */
    public T getRecord(int index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("Record index out of range: " + index);
        }
        return records[index];
    }

    /**
     * Удалить запись по индексу слота.
     * @return true, если запись была, false если и так пусто.
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
     * Сериализация блока в массив байтов фиксированной длины.
     */
    public byte[] toByteArray() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // 1) validCount
            dos.writeInt(validCount);

            // 2) слоты
            for (int i = 0; i < capacity; i++) {
                T rec = records[i];
                if (rec != null) {
                    dos.writeByte(1); // используется
                    byte[] data = rec.toByteArray();
                    if (data.length != recordSize) {
                        throw new IllegalStateException(
                                "Record serialized size (" + data.length +
                                ") != expected recordSize (" + recordSize + ")");
                    }
                    dos.write(data);
                } else {
                    dos.writeByte(0); // пустой слот
                    // записываем пустые байты чтобы сохранить фиксированный размер блока
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
     * Десериализация блока из массива байтов.
     * Размер массива должен соответствовать полному размеру блока.
     */
    public void fromByteArray(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            // 1) validCount
            this.validCount = dis.readInt();

            // 2) слоты
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
     * Внутренний хелпер: создать пустой экземпляр T через reflection.
     */
    private T createEmptyRecord() {
        try {
            return recordClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create instance of record class " + recordClass.getName(), e);
        }
    }

    /**
     * Удобная строка для отладочного вывода содержимого блока.
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
