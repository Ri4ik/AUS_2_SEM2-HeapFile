package aus2_sem2.model;

import aus2_sem2.util.ByteUtils;

/**
 * Model jedného záznamu pacienta uloženého v heap súbore.
 * Každý atribút má pevnú dĺžku kvôli jednoduchému ukladaniu do blokov.
 */
public class PatientRecord implements Record {

    private String meno;        // max 15 znakov
    private String priezvisko;  // max 14 znakov
    private String date;        // "DD:MM:RRRR" – 10 znakov
    private String id;          // unikátne ID pacienta – 10 znakov

    public static final int MENO_LEN = 15;
    public static final int PRIEZ_LEN = 14;
    public static final int DATE_LEN = 10;
    public static final int ID_LEN = 10;

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

    /** Veľkosť záznamu v bajtoch (súčet pevných dĺžok). */
    @Override
    public int getSize() {
        return MENO_LEN + PRIEZ_LEN + DATE_LEN + ID_LEN;
    }

    /** Serializácia záznamu do pevného bajtového poľa. */
    @Override
    public byte[] toByteArray() {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);

            dos.write(ByteUtils.toFixedBytes(meno, MENO_LEN));
            dos.write(ByteUtils.toFixedBytes(priezvisko, PRIEZ_LEN));
            dos.write(ByteUtils.toFixedBytes(date, DATE_LEN));
            dos.write(ByteUtils.toFixedBytes(id, ID_LEN));

            return baos.toByteArray();

        } catch (java.io.IOException e) {
            throw new IllegalStateException("Error during toByteArray()");
        }
    }

    /** Načítanie záznamu z pevného bajtového poľa. */
    @Override
    public void fromByteArray(byte[] data) {
        try {
            int pos = 0;

            this.meno = ByteUtils.fromFixedBytes(data, pos, MENO_LEN);
            pos += MENO_LEN;

            this.priezvisko = ByteUtils.fromFixedBytes(data, pos, PRIEZ_LEN);
            pos += PRIEZ_LEN;

            this.date = ByteUtils.fromFixedBytes(data, pos, DATE_LEN);
            pos += DATE_LEN;

            this.id = ByteUtils.fromFixedBytes(data, pos, ID_LEN);

        } catch (Exception e) {
            throw new IllegalStateException("Error during fromByteArray()");
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

    /** Používa sa pri kontrole unikátneho ID. */
    @Override
    public String getId() {
        return id;
    }
}
