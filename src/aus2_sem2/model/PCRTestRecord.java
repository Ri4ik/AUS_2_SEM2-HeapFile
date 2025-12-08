package aus2_sem2.model;

import aus2_sem2.util.ByteUtils;

/**
 * Model jedného záznamu PCR testu uloženého v heap súbore.
 *
 * Každý textový atribút sa ukladá ako:
 *   1 bajt – počet platných znakov,
 *   N bajtov – dáta v pevnej dĺžke (padding nulami).
 *
 * Polia podľa zadania:
 *  - dátum a čas testu          -> dateTime (napr. "DD:MM:RRRR HH:MM")
 *  - unikátne číslo pacienta    -> patientId (max 10 znakov)
 *  - unikátny kód PCR testu     -> testCode (int)
 *  - výsledok testu             -> result (boolean)
 *  - hodnota testu              -> value (double)
 *  - poznámka                   -> note (max 11 znakov)
 */
public class PCRTestRecord implements Record {

    private String dateTime;   // napr. "01:01:2025 13:45" – max 16 znakov
    private String patientId;  // unikátne číslo pacienta – max 10 znakov
    private int    testCode;   // unikátny kód PCR testu
    private boolean result;    // výsledok testu (pozitívny/negatívny)
    private double value;      // hodnota testu
    private String note;       // poznámka – max 11 znakov

    // --- konštanty fixných dĺžok pre textové polia ---

    public static final int DATETIME_LEN   = 16; // "DD:MM:RRRR HH:MM"
    public static final int PATIENT_ID_LEN = 10;
    public static final int NOTE_LEN       = 11;

    private static final int DATETIME_FIELD_SIZE   = 1 + DATETIME_LEN;
    private static final int PATIENT_ID_FIELD_SIZE = 1 + PATIENT_ID_LEN;
    private static final int NOTE_FIELD_SIZE       = 1 + NOTE_LEN;

    public PCRTestRecord() {
        this.dateTime  = "";
        this.patientId = "";
        this.testCode  = 0;
        this.result    = false;
        this.value     = 0.0;
        this.note      = "";
    }

    public PCRTestRecord(String dateTime,
                         String patientId,
                         int testCode,
                         boolean result,
                         double value,
                         String note) {
        this.dateTime  = dateTime;
        this.patientId = patientId;
        this.testCode  = testCode;
        this.result    = result;
        this.value     = value;
        this.note      = note;
    }

    // =========================================================
    //  Record API
    // =========================================================

    /**
     * Fixná veľkosť záznamu v bajtoch.
     *
     * Štruktúra:
     *  - dateTime:   1 + DATETIME_LEN
     *  - patientId:  1 + PATIENT_ID_LEN
     *  - testCode:   4 (int)
     *  - result:     1 (boolean)
     *  - value:      8 (double)
     *  - note:       1 + NOTE_LEN
     */
    @Override
    public int getSize() {
        return DATETIME_FIELD_SIZE
                + PATIENT_ID_FIELD_SIZE
                + Integer.BYTES   // testCode
                + 1               // result (boolean)
                + Double.BYTES    // value
                + NOTE_FIELD_SIZE;
    }

    /**
     * Serializácia záznamu do pevnej dĺžky bajtového poľa.
     *
     * Každý String:
     *   - uložíme dĺžku v znakoch (max N),
     *   - uložíme fixné pole bajtov s paddingom nulami.
     */
    @Override
    public byte[] toByteArray() {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);

            // 1) dateTime
            String dtStr = (dateTime == null) ? "" : dateTime;
            byte[] dtBytes = ByteUtils.toFixedBytes(dtStr, DATETIME_LEN);
            int dtLen = Math.min(dtStr.length(), DATETIME_LEN); // počet platných znakov
            dos.writeByte(dtLen);
            dos.write(dtBytes);

            // 2) patientId (unikátne číslo pacienta)
            String pid = (patientId == null) ? "" : patientId;
            byte[] pidBytes = ByteUtils.toFixedBytes(pid, PATIENT_ID_LEN);
            int pidLen = Math.min(pid.length(), PATIENT_ID_LEN);
            dos.writeByte(pidLen);
            dos.write(pidBytes);

            // 3) testCode (int)
            dos.writeInt(testCode);

            // 4) result (boolean)
            dos.writeBoolean(result);

            // 5) value (double)
            dos.writeDouble(value);

            // 6) note
            String n = (note == null) ? "" : note;
            byte[] noteBytes = ByteUtils.toFixedBytes(n, NOTE_LEN);
            int noteLen = Math.min(n.length(), NOTE_LEN);
            dos.writeByte(noteLen);
            dos.write(noteBytes);

            return baos.toByteArray();

        } catch (java.io.IOException e) {
            throw new IllegalStateException("Error during PCRTestRecord.toByteArray()", e);
        }
    }

    /**
     * Načítanie záznamu z pevne veľkého bajtového poľa.
     * Dôležité: pri načítaní reálne používame uloženú dĺžku stringu.
     */
    @Override
    public void fromByteArray(byte[] data) {
        try {
            java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
            java.io.DataInputStream dis = new java.io.DataInputStream(bais);

            // 1) dateTime
            int dtLen = dis.readUnsignedByte(); // uložený počet platných znakov
            byte[] dtBytes = new byte[DATETIME_LEN];
            dis.readFully(dtBytes);
            String dtFull = ByteUtils.fromFixedBytes(dtBytes, 0, DATETIME_LEN);
            if (dtLen < dtFull.length()) {
                dtFull = dtFull.substring(0, dtLen);
            }
            this.dateTime = dtFull;

            // 2) patientId
            int pidLen = dis.readUnsignedByte();
            byte[] pidBytes = new byte[PATIENT_ID_LEN];
            dis.readFully(pidBytes);
            String pidFull = ByteUtils.fromFixedBytes(pidBytes, 0, PATIENT_ID_LEN);
            if (pidLen < pidFull.length()) {
                pidFull = pidFull.substring(0, pidLen);
            }
            this.patientId = pidFull;

            // 3) testCode
            this.testCode = dis.readInt();

            // 4) result
            this.result = dis.readBoolean();

            // 5) value
            this.value = dis.readDouble();

            // 6) note
            int noteLen = dis.readUnsignedByte();
            byte[] noteBytes = new byte[NOTE_LEN];
            dis.readFully(noteBytes);
            String noteFull = ByteUtils.fromFixedBytes(noteBytes, 0, NOTE_LEN);
            if (noteLen < noteFull.length()) {
                noteFull = noteFull.substring(0, noteLen);
            }
            this.note = noteFull;

        } catch (Exception e) {
            throw new IllegalStateException("Error during PCRTestRecord.fromByteArray()", e);
        }
    }

    /**
     * Porovnanie dvoch záznamov PCRTestRecord podľa všetkých atribútov.
     */
    @Override
    public boolean equals(Record other) {
        if (!(other instanceof PCRTestRecord)) return false;
        PCRTestRecord o = (PCRTestRecord) other;

        if (this.testCode != o.testCode) return false;
        if (this.result != o.result)     return false;
        if (Double.compare(this.value, o.value) != 0) return false;

        if (!safeEquals(this.dateTime,  o.dateTime))  return false;
        if (!safeEquals(this.patientId, o.patientId)) return false;
        if (!safeEquals(this.note,      o.note))      return false;

        return true;
    }

    private static boolean safeEquals(String a, String b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }

    /**
     * Kľúč pre indexovanie v LinHashFile – unikátny kód PCR testu.
     */
    @Override
    public String getId() {
        return Integer.toString(testCode);
        // Ak chceš fixnú šírku (napr. 10 číslic), môžeš použiť:
        // return String.format("%010d", testCode);
    }

    // =========================================================
    //  Gettre / settre pre GUI a service vrstvu
    // =========================================================

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public String getPatientId() {
        return patientId;
    }

    public void setPatientId(String patientId) {
        this.patientId = patientId;
    }

    public int getTestCode() {
        return testCode;
    }

    public void setTestCode(int testCode) {
        this.testCode = testCode;
    }

    public boolean isResult() {
        return result;
    }

    public void setResult(boolean result) {
        this.result = result;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    // pekný toString pre debug / GUI
    @Override
    public String toString() {
        return "PCRTestRecord{" +
                "dateTime='" + dateTime + '\'' +
                ", patientId='" + patientId + '\'' +
                ", testCode=" + testCode +
                ", result=" + result +
                ", value=" + value +
                ", note='" + note + '\'' +
                '}';
    }
}
