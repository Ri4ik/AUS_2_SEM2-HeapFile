package aus2_sem2.controller;

import aus2_sem2.model.PatientRecord;
import aus2_sem2.storage.HeapFile;

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

    public PatientFileController(HeapFile<PatientRecord> heapFile) {
        this.heapFile = heapFile;
        this.random = new Random();
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

    // --- obyčajné vkladanie (bez kontroly ID) ---

    /** Vloží náhodné záznamy bez unikátnej kontroly. */
    public void insertRandomRecords(int count) {
        for (int i = 0; i < count; i++) {
            PatientRecord rec = generateRandomRecord();
            heapFile.insert(rec);
        }
    }

    /** Vloží jeden záznam bez kontroly ID. */
    public long insertRecord(PatientRecord rec) {
        return heapFile.insert(rec);
    }

    // --- mazanie (náhodné alebo podľa ID) ---

    /** Zmaže náhodné záznamy zo súboru. */
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

    /** Zmaže prvý záznam so zadaným ID. */
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

    /** Otestuje základnú funkcionalitu vkladania/mazania. */
    public void runFunctionalTest() {
        List<Long> addrs = new ArrayList<>();

        // vloží 10 testovacích záznamov
        for (int i = 0; i < 10; i++) {
            String meno = "TestM" + i;
            String priez = "TestP" + i;
            String date = String.format("%02d:%02d:%04d", (i % 28) + 1, (i % 12) + 1, 2000 + i);
            String id = String.format("TST%07d", i);

            PatientRecord rec = new PatientRecord(meno, priez, date, id);
            long addr = heapFile.insert(rec);
            addrs.add(addr);
        }

        // zmaže 4 z nich
        int[] toDeleteIdx = {1, 3, 5, 7};
        for (int idx : toDeleteIdx) {
            if (idx < addrs.size()) {
                heapFile.delete(addrs.get(idx));
            }
        }
    }

    // --- unikátne vkladanie ---

    /** Vloží záznam len ak ID nie je v systéme. */
    public long insertRecordUnique(PatientRecord rec) {
        return heapFile.insertUnique(rec); // -1 ak ID existuje
    }

    /** Vloží náhodné unikátne záznamy. */
    public int insertRandomUniqueRecords(int count) {
        int inserted = 0;
        int triesLimit = count * 20;
        int tries = 0;

        while (inserted < count && tries < triesLimit) {
            PatientRecord rec = generateRandomRecord();
            long addr = heapFile.insertUnique(rec);
            if (addr != -1L) inserted++;
            tries++;
        }
        return inserted;
    }

    // --- pomocné generovanie dát ---

    /** Vytvorí náhodný záznam – používa sa pri testovaní. */
    private PatientRecord generateRandomRecord() {
        int year = 1950 + random.nextInt(60);
        int month = 1 + random.nextInt(12);
        int day = 1 + random.nextInt(28);

        String date = String.format("%02d:%02d:%04d", day, month, year);
        String meno = "M" + random.nextInt(100000);
        String priezvisko = "P" + random.nextInt(100000);
        String id = String.format("R%07d", random.nextInt(10_000_000));

        return new PatientRecord(meno, priezvisko, date, id);
    }
}
