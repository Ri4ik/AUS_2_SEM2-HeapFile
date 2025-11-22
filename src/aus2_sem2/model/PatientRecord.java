package aus2_sem2.model;

import aus2_sem2.util.ByteUtils;
import java.io.*;

public class PatientRecord implements Record {

    private String meno;        // 15 chars
    private String priezvisko;  // 14 chars
    private int year;
    private int month;
    private int day;
    private String id;          // 10 chars

    public static final int MENO_LEN = 15;
    public static final int PRIEZ_LEN = 14;
    public static final int ID_LEN = 10;

    public PatientRecord() {
        this.meno = "";
        this.priezvisko = "";
        this.id = "";
    }

    public PatientRecord(String meno, String priezvisko, int year, int month, int day, String id) {
        this.meno = meno;
        this.priezvisko = priezvisko;
        this.year = year;
        this.month = month;
        this.day = day;
        this.id = id;
    }

    @Override
    public int getSize() {
        return MENO_LEN + PRIEZ_LEN + ID_LEN + (3 * 4);
    }

    @Override
    public byte[] toByteArray() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.write(ByteUtils.toFixedBytes(meno, MENO_LEN));
            dos.write(ByteUtils.toFixedBytes(priezvisko, PRIEZ_LEN));
            dos.writeInt(year);
            dos.writeInt(month);
            dos.writeInt(day);
            dos.write(ByteUtils.toFixedBytes(id, ID_LEN));

            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Error during toByteArray()");
        }
    }

    @Override
    public void fromByteArray(byte[] data) {
        try {
            int pos = 0;

            this.meno = ByteUtils.fromFixedBytes(data, pos, MENO_LEN);
            pos += MENO_LEN;

            this.priezvisko = ByteUtils.fromFixedBytes(data, pos, PRIEZ_LEN);
            pos += PRIEZ_LEN;

            ByteArrayInputStream bais = new ByteArrayInputStream(data, pos, 12);
            DataInputStream dis = new DataInputStream(bais);

            this.year = dis.readInt();
            this.month = dis.readInt();
            this.day = dis.readInt();
            pos += 12;

            this.id = ByteUtils.fromFixedBytes(data, pos, ID_LEN);

        } catch (Exception e) {
            throw new IllegalStateException("Error during fromByteArray()");
        }
    }

    @Override
    public boolean equals(Record other) {
        if (!(other instanceof PatientRecord)) return false;
        PatientRecord p = (PatientRecord) other;

        return this.meno.equals(p.meno)
                && this.priezvisko.equals(p.priezvisko)
                && this.year == p.year
                && this.month == p.month
                && this.day == p.day
                && this.id.equals(p.id);
    }

    @Override
    public String toString() {
        return meno + " " + priezvisko + " (" + day + "." + month + "." + year + ") ID=" + id;
    }
}
