package aus2_sem2.model;

import aus2_sem2.util.ByteUtils;

/**
 * Model jedného záznamu pacienta uloženého v heap súbore.
 * Každý textový atribút sa ukladá ako:
 *   1 bajt – počet platných znakov,
 *   N bajtov – dáta v pevnej dĺžke (padding nulami).
 */
public class PatientRecord implements Record {

    private String meno;        // max 15 znakov
    private String priezvisko;  // max 14 znakov
    private String date;        // "DD:MM:RRRR" – 10 znakov
    private String id;          // unikátne ID pacienta – 10 znakov

    public static final int MENO_LEN  = 15;
    public static final int PRIEZ_LEN = 14;
    public static final int DATE_LEN  = 10;
    public static final int ID_LEN    = 10;

    // každý string: 1 bajt dĺžka + fixné pole bajtov
    private static final int MENO_FIELD_SIZE  = 1 + MENO_LEN;
    private static final int PRIEZ_FIELD_SIZE = 1 + PRIEZ_LEN;
    private static final int DATE_FIELD_SIZE  = 1 + DATE_LEN;
    private static final int ID_FIELD_SIZE    = 1 + ID_LEN;

    public PatientRecord() {
        this.meno = "";
        this.priezvisko = "";
        this.date = "";
        this.id = "";
    }

    public PatientRecord(String meno, String priezvisko, String date, String id) {
        this.meno = meno;
        this.priezvisko = priezvisko;
        this.date = date;
        this.id = id;
    }

    /** Fixná veľkosť záznamu v bajtoch. */
    @Override
    public int getSize() {
        // (1 + MENO_LEN) + (1 + PRIEZ_LEN) + (1 + DATE_LEN) + (1 + ID_LEN)
        return MENO_FIELD_SIZE + PRIEZ_FIELD_SIZE + DATE_FIELD_SIZE + ID_FIELD_SIZE;
    }

    /** Serializácia záznamu – ku každému stringu sa uloží aj dĺžka. */
    @Override
    public byte[] toByteArray() {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);

            // meno
            String menoStr = (meno == null) ? "" : meno;
            byte[] menoBytes = ByteUtils.toFixedBytes(menoStr, MENO_LEN);
            int menoLen = Math.min(menoStr.length(), MENO_LEN);
            dos.writeByte(menoLen);
            dos.write(menoBytes);

            // priezvisko
            String priezStr = (priezvisko == null) ? "" : priezvisko;
            byte[] priezBytes = ByteUtils.toFixedBytes(priezStr, PRIEZ_LEN);
            int priezLen = Math.min(priezStr.length(), PRIEZ_LEN);
            dos.writeByte(priezLen);
            dos.write(priezBytes);

            // date
            String dateStr = (date == null) ? "" : date;
            byte[] dateBytes = ByteUtils.toFixedBytes(dateStr, DATE_LEN);
            int dateLen = Math.min(dateStr.length(), DATE_LEN);
            dos.writeByte(dateLen);
            dos.write(dateBytes);

            // id
            String idStr = (id == null) ? "" : id;
            byte[] idBytes = ByteUtils.toFixedBytes(idStr, ID_LEN);
            int idLen = Math.min(idStr.length(), ID_LEN);
            dos.writeByte(idLen);
            dos.write(idBytes);

            return baos.toByteArray();

        } catch (java.io.IOException e) {
            throw new IllegalStateException("Error during PatientRecord.toByteArray()", e);
        }
    }

    /** Načítanie záznamu – číta dĺžku + fixné bajty pre každý reťazec a dĺžku reálne používa. */
    @Override
    public void fromByteArray(byte[] data) {
        try {
            java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
            java.io.DataInputStream dis = new java.io.DataInputStream(bais);

            // meno
            int menoLen = dis.readUnsignedByte(); // počet platných znakov
            byte[] menoBytes = new byte[MENO_LEN];
            dis.readFully(menoBytes);
            String menoFull = ByteUtils.fromFixedBytes(menoBytes, 0, MENO_LEN);
            if (menoLen < menoFull.length()) {
                menoFull = menoFull.substring(0, menoLen);
            }
            this.meno = menoFull;

            // priezvisko
            int priezLen = dis.readUnsignedByte();
            byte[] priezBytes = new byte[PRIEZ_LEN];
            dis.readFully(priezBytes);
            String priezFull = ByteUtils.fromFixedBytes(priezBytes, 0, PRIEZ_LEN);
            if (priezLen < priezFull.length()) {
                priezFull = priezFull.substring(0, priezLen);
            }
            this.priezvisko = priezFull;

            // date
            int dateLen = dis.readUnsignedByte();
            byte[] dateBytes = new byte[DATE_LEN];
            dis.readFully(dateBytes);
            String dateFull = ByteUtils.fromFixedBytes(dateBytes, 0, DATE_LEN);
            if (dateLen < dateFull.length()) {
                dateFull = dateFull.substring(0, dateLen);
            }
            this.date = dateFull;

            // id
            int idLen = dis.readUnsignedByte();
            byte[] idBytes = new byte[ID_LEN];
            dis.readFully(idBytes);
            String idFull = ByteUtils.fromFixedBytes(idBytes, 0, ID_LEN);
            if (idLen < idFull.length()) {
                idFull = idFull.substring(0, idLen);
            }
            this.id = idFull;

        } catch (Exception e) {
            throw new IllegalStateException("Error during PatientRecord.fromByteArray()", e);
        }
    }

    /** Porovnanie dvoch záznamov podľa všetkých atribútov. */
    @Override
    public boolean equals(Record other) {
        if (!(other instanceof PatientRecord)) return false;
        PatientRecord p = (PatientRecord) other;

        return this.meno.equals(p.meno)
                && this.priezvisko.equals(p.priezvisko)
                && this.date.equals(p.date)
                && this.id.equals(p.id);
    }

    /** Textová reprezentácia záznamu pre GUI a debug. */
    @Override
    public String toString() {
        return meno + " " + priezvisko + " (" + date + ") ID=" + id;
    }

    // --- gettre pre GUI, controller aj HeapFile ---

    public String getMeno() {
        return meno;
    }

    public String getPriezvisko() {
        return priezvisko;
    }

    public String getDate() {
        return date;
    }

    /** Používa sa pri kontrole/ukladaní ID. */
    @Override
    public String getId() {
        return id;
    }
}
