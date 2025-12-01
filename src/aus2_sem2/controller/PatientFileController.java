package aus2_sem2.controller;

import aus2_sem2.model.PatientRecord;
import aus2_sem2.storage.HeapFile;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

/**
 * Controller – sprostredkuje prácu medzi GUI a HeapFile.
 * GUI nikdy nepracuje s dátami priamo, ale cez tento kontrolér.
 */
public class PatientFileController {

    private final HeapFile<PatientRecord> heapFile; // model – súbor na disku
    private final Random random;

    // auto-increment ID pacienta (unikátne v rámci systému)
    private static final String ID_META_FILE = "heapfile_patients_id.meta";

    /**
     * Počítadlo ďalšieho ID pacienta.
     * Používame typ int ako "nezáporný" čítač (prakticky stačí rozsah).
     */
    private int nextPatientId;

    public PatientFileController(HeapFile<PatientRecord> heapFile) {
        this.heapFile = heapFile;
        this.random = new Random();
        this.nextPatientId = loadNextPatientId(); // načítame posledné ID alebo 1
    }

    // --- info pre GUI ---

    /** Vráti textový dump dátového súboru. */
    public String getDump() {
        return heapFile.dumpDebugInfo();
    }

    /** Vráti počet platných záznamov. */
    public int getTotalRecords() {
        return heapFile.getTotalValidRecords();
    }

    // --- mazanie (náhodné alebo podľa ID) ---

    /**
     * Zmaže náhodné záznamy zo súboru.
     * V GUI sa používa na rýchle otestovanie mazania.
     */
    public int deleteRandomRecords(int count) {
        List<Long> addresses = heapFile.getAllAddresses();
        if (addresses.isEmpty() || count <= 0) return 0;
        if (count > addresses.size()) count = addresses.size();

        Collections.shuffle(addresses, random);

        int removed = 0;
        for (int i = 0; i < count; i++) {
            long addr = addresses.get(i);
            if (heapFile.delete(addr)) removed++;
        }
        return removed;
    }

    /**
     * Zmaže prvý záznam so zadaným ID.
     * Používa sa v GUI pri mazaní podľa ID pacienta.
     */
    public boolean deleteById(String id) {
        List<Long> addresses = heapFile.getAllAddresses();
        for (long addr : addresses) {
            PatientRecord rec = heapFile.get(addr);
            if (rec != null && id.equals(rec.getId())) {
                return heapFile.delete(addr);
            }
        }
        return false;
    }

    // --- funkčný test (10 insert, 4 delete) ---

    /**
     * Otestuje základnú funkcionalitu vkladania/mazania.
     * Všetky vkladané záznamy používajú auto-increment ID (insertRecordUnique).
     */
    public void runFunctionalTest() {
        List<Long> addrs = new ArrayList<>();

        // vloží 10 testovacích záznamov s auto-ID
        for (int i = 0; i < 10; i++) {
            String meno = "TestM" + i;
            String priez = "TestP" + i;
            String date = String.format("%02d:%02d:%04d",
                    (i % 28) + 1, (i % 12) + 1, 2000 + i);

            // ID sa ponechá prázdne – controller ho doplní auto-increment hodnotou
            PatientRecord recWithoutId = new PatientRecord(meno, priez, date, "");
            long addr = insertRecordUnique(recWithoutId);
            addrs.add(addr);
        }

        // zmaže 4 z nich priamo cez adresu
        int[] toDeleteIdx = {1, 3, 5, 7};
        for (int idx : toDeleteIdx) {
            if (idx < addrs.size()) {
                heapFile.delete(addrs.get(idx));
            }
        }
    }

    // --- unikátne vkladanie s auto-increment ID ---

    /**
     * Vloží pacienta tak, že ID sa VŽDY generuje automaticky (auto-increment),
     * používateľom zadané ID sa ignoruje.
     *
     * @param recWithoutId záznam s vyplneným menom, priezviskom a dátumom,
     *                     pole ID sa ignoruje (môže byť prázdne)
     * @return adresa vloženého záznamu v HeapFile
     */
    public long insertRecordUnique(PatientRecord recWithoutId) {
        String newId = generateNextIdString();
        PatientRecord withId = new PatientRecord(
                recWithoutId.getMeno(),
                recWithoutId.getPriezvisko(),
                recWithoutId.getDate(),
                newId
        );
        long addr = heapFile.insert(withId);
        saveNextPatientId(); // uložíme aktuálnu hodnotu počítadla
        return addr;
    }

    /**
     * Vloží náhodné unikátne záznamy – ID sú generované z počítadla,
     * nie náhodne. Zabezpečuje sa tým unikátnosť v rámci systému.
     */
    public int insertRandomUniqueRecords(int count) {
        int inserted = 0;
        for (int i = 0; i < count; i++) {
            PatientRecord base = generateRandomRecordWithoutId();
            String newId = generateNextIdString();
            PatientRecord withId = new PatientRecord(
                    base.getMeno(),
                    base.getPriezvisko(),
                    base.getDate(),
                    newId
            );
            heapFile.insert(withId);
            inserted++;
        }
        saveNextPatientId();
        return inserted;
    }

    // --- pomocné generovanie dát ---

    /**
     * Vytvorí náhodný záznam pacienta BEZ ID.
     * ID sa doplní až pri vkladaní (auto-increment).
     */
    private PatientRecord generateRandomRecordWithoutId() {
        int year = 1950 + random.nextInt(60);
        int month = 1 + random.nextInt(12);
        int day = 1 + random.nextInt(28);

        String date = String.format("%02d:%02d:%04d", day, month, year);
        String meno = "M" + random.nextInt(100000);
        String priezvisko = "P" + random.nextInt(100000);

        // ID nechávame prázdne – doplní sa auto-incrementom
        return new PatientRecord(meno, priezvisko, date, "");
    }

    // --- auto-increment ID pomocné metódy ---

    /**
     * Načíta nextPatientId z meta súboru alebo vráti 1, ak meta súbor neexistuje
     * alebo obsahuje neplatnú hodnotu.
     */
    private int loadNextPatientId() {
        File f = new File(ID_META_FILE);
        if (!f.exists()) {
            return 1;
        }
        try (DataInputStream dis = new DataInputStream(new FileInputStream(f))) {
            int value = dis.readInt();
            if (value <= 0) {
                return 1;
            }
            return value;
        } catch (IOException e) {
            // v prípade problému začneme od 1
            return 1;
        }
    }

    /**
     * Uloží aktuálnu hodnotu nextPatientId do meta súboru.
     */
    private void saveNextPatientId() {
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(ID_META_FILE))) {
            dos.writeInt(nextPatientId);
        } catch (IOException e) {
            // pri zlyhaní meta súboru nechceme zabiť aplikáciu, chybu ignorujeme
        }
    }

    /**
     * Vygeneruje nový ID reťazec v tvare "0000000001", "0000000002", ...
     * a inkrementuje počítadlo.
     */
    private String generateNextIdString() {
        if (nextPatientId <= 0) {
            // ochrana pre prípad poškodenia meta súboru alebo overflow
            nextPatientId = 1;
        }
        String result = String.format("%010d", nextPatientId);
        nextPatientId++;

        // jednoduchá ochrana pred pretečením – po MAX_VALUE začneme od 1,
        // v reálnej úlohe sa na tento limit prakticky nedostaneme
        if (nextPatientId == Integer.MIN_VALUE) {
            nextPatientId = 1;
        }

        return result;
    }

    /** Volateľné pri korektnom ukončení aplikácie. */
    public void close() {
        saveNextPatientId();
        heapFile.close();
    }
}
