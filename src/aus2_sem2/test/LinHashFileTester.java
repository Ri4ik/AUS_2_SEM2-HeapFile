package aus2_sem2.test;

import aus2_sem2.model.PatientRecord;
import aus2_sem2.storage.HeapFile;
import aus2_sem2.storage.LinHashFile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Tester pre LinHashFile<PatientRecord> (lineárne hešovanie nad HeapFile).
 *
 * - Používa samostatný heap súbor a meta súbor (neovplyvní produkčné dáta).
 * - Vloží 10000 záznamov cez LinHashFile.insert().
 * - Vyhľadá 2000 záznamov cez LinHashFile.findById().
 * - Zmaže 5000 záznamov cez LinHashFile.deleteById().
 * - Overí konzistenciu nájdených/zmazaných záznamov.
 * - Otestuje opätovné otvorenie LinHashFile (loadMeta + rebuildDirectory).
 *
 * Pri akejkoľvek nezrovnalosti vyhodí IllegalStateException z testera.
 */
public class LinHashFileTester {

    private static final int INSERT_COUNT = 10_000;
    private static final int FIND_COUNT   = 2_000;
    private static final int DELETE_COUNT = 1_000;

    /**
     * Spustí kompletný testovací scenár pre LinHashFile.
     */
    public static void runAllTests() {
        String heapPath = "heapfile_linhash_test.dat";
        String metaPath = "patients_linhash_test.meta";

        deleteIfExists(heapPath);
        deleteIfExists(metaPath);

        HeapFile<PatientRecord> heap =
                new HeapFile<>(heapPath, 256, PatientRecord.class);
        LinHashFile<PatientRecord> lin =
                new LinHashFile<>(heap, metaPath);

        try {
            System.out.println("=== LinHashFileTester: START ===");
            testInsertAndFind(heap, lin);
            testDelete(heap, lin);
            testReopen(heapPath, metaPath);
            System.out.println("=== LinHashFileTester: OK ===");
        } finally {
            // korektné ukončenie (uloží meta + zatvorí HeapFile)
            lin.close();
        }
    }

    // =======================
    // 1. Vkladanie + hľadanie
    // =======================

    private static void testInsertAndFind(HeapFile<PatientRecord> heap,
                                          LinHashFile<PatientRecord> lin) {
        System.out.println("[LinHashFileTester] Insert " + INSERT_COUNT + " records...");

        List<String> ids = new ArrayList<>(INSERT_COUNT);

        for (int i = 0; i < INSERT_COUNT; i++) {
            PatientRecord rec = generateRecord(i);
            long addr = lin.insert(rec);
            if (addr < 0) {
                fail("LinHashFile.insert vrátil zápornú adresu pre i=" + i);
            }
            ids.add(rec.getId());
        }

        int total = heap.getTotalValidRecords();
        assertEquals(INSERT_COUNT, total,
                "Po vkladaní cez LinHashFile je v HeapFile nesprávny počet záznamov.");

       if (FIND_COUNT > INSERT_COUNT) {
            fail("FIND_COUNT (" + FIND_COUNT + ") je väčší ako INSERT_COUNT (" + INSERT_COUNT + ") "
                 + "v LinHashFileTester.");
        }

        System.out.println("[LinHashFileTester] Find " + FIND_COUNT + " records...");
        for (int i = 0; i < FIND_COUNT; i++) {
            String id = ids.get(i);
            PatientRecord found = lin.findById(id);
            if (found == null) {
                fail("findById(" + id + ") vrátil null po vkladaní.");
            }
            if (!id.equals(found.getId())) {
                fail("findById(" + id + ") vrátil záznam s iným ID: " + found.getId());
            }
        }

        System.out.println("[LinHashFileTester] Insert/Find OK");
         // dodatočná kontrola konzistencie po vkladaní a hľadaní
        validateConsistency(heap, lin);
    }

    // =======================
    // 2. Mazanie
    // =======================

    private static void testDelete(HeapFile<PatientRecord> heap,
                                   LinHashFile<PatientRecord> lin) {
        System.out.println("[LinHashFileTester] Delete " + DELETE_COUNT + " records...");

        // znovu si pripravíme ID všetkých záznamov (pre jednoduchosť)
        // v reálnej aplikácii by sme si ich uchovali pri vkladaní
        List<Long> allAddrs = heap.getAllAddresses();
        List<String> allIds = new ArrayList<>(allAddrs.size());

        for (long addr : allAddrs) {
            PatientRecord rec = heap.get(addr);
            if (rec == null) {
                fail("Na adrese " + addr + " je null ešte pred mazaním LinHashFile.");
            }
            allIds.add(rec.getId());
        }

        int totalBefore = heap.getTotalValidRecords();
        if (DELETE_COUNT > totalBefore) {
            fail("DELETE_COUNT je väčší než počet záznamov v HeapFile.");
        }

        List<String> deleteIds = new ArrayList<>(DELETE_COUNT);
        List<String> keepIds = new ArrayList<>(totalBefore - DELETE_COUNT);

        for (int i = 0; i < allIds.size(); i++) {
            String id = allIds.get(i);
            if (i < DELETE_COUNT) {
                deleteIds.add(id);
            } else {
                keepIds.add(id);
            }
        }

        // fyzické mazanie cez LinHashFile.deleteById
        for (int i = 0; i < DELETE_COUNT; i++) {
            String id = deleteIds.get(i);
            boolean deleted = lin.deleteById(id);
            if (!deleted) {
                fail("deleteById(" + id + ") v LinHashFile vrátil false.");
            }
        }

        int totalAfter = heap.getTotalValidRecords();
        int expectedAfter = totalBefore - DELETE_COUNT;
        assertEquals(expectedAfter, totalAfter,
                "Po mazaní cez LinHashFile má HeapFile nesprávny počet záznamov.");

        // overíme, že zmazané ID naozaj nie sú nájditeľné
        for (String id : deleteIds) {
            PatientRecord rec = lin.findById(id);
            if (rec != null) {
                fail("findById(" + id + ") po mazaní stále vrátil záznam: " + rec);
            }
        }

        // overíme, že nezmazané ID sú stále nájditeľné
        for (String id : keepIds) {
            PatientRecord rec = lin.findById(id);
            if (rec == null) {
                fail("findById(" + id + ") po mazaní vrátil null, ale ID bolo ponechané.");
            }
            if (!id.equals(rec.getId())) {
                fail("Po mazaní: findById(" + id + ") vrátil záznam s iným ID: " + rec.getId());
            }
        }

        System.out.println("[LinHashFileTester] Delete OK");
         // dodatočná kontrola konzistencie po vkladaní a hľadaní
        validateConsistency(heap, lin);
    }

    // =======================
    // 3. Reopen test
    // =======================

    private static void testReopen(String heapPath, String metaPath) {
        System.out.println("[LinHashFileTester] Reopen LinHashFile...");

        // otvoríme existujúci HeapFile a LinHashFile
        HeapFile<PatientRecord> heap =
                new HeapFile<>(heapPath, 256, PatientRecord.class);
        LinHashFile<PatientRecord> lin =
                new LinHashFile<>(heap, metaPath);

        try {
            int total = heap.getTotalValidRecords();

            // zistíme ID všetkých záznamov
            List<Long> addrs = heap.getAllAddresses();
            List<String> ids = new ArrayList<>(addrs.size());
            for (long addr : addrs) {
                PatientRecord rec = heap.get(addr);
                if (rec == null) {
                    fail("Po opätovnom otvorení: null na adrese " + addr);
                }
                ids.add(rec.getId());
            }

            // preskúšame findById pre všetky ID – ak niečo zlyhá, rehash/rebuild je zlý
            for (String id : ids) {
                PatientRecord found = lin.findById(id);
                if (found == null) {
                    fail("Po opätovnom otvorení: findById(" + id + ") vrátil null.");
                }
                if (!id.equals(found.getId())) {
                    fail("Po opätovnom otvorení: findById(" + id +
                            ") vrátil záznam s iným ID: " + found.getId());
                }
            }

            // jednoduchá kontrola, že počet sedí
            int total2 = heap.getTotalValidRecords();
            assertEquals(total, total2,
                    "Po opätovnom otvorení LinHashFile/HeapFile nesedí totalValidRecords().");
             // dodatočná kontrola konzistencie po vkladaní a hľadaní
            validateConsistency(heap, lin);
            System.out.println("[LinHashFileTester] Reopen OK");
        } finally {
            lin.close(); // uloží meta + zatvorí HeapFile
        }
    }

    // =======================
    // Pomocné metódy
    // =======================

    private static PatientRecord generateRecord(int i) {
        String meno = "LH_M" + i;
        String priez = "LH_P" + i;
        String date = String.format("%02d:%02d:%04d",
                (i % 28) + 1,
                (i % 12) + 1,
                1980 + (i % 40));
        String id = String.format("LH%07d", i); // jednoznačné ID pre test
        return new PatientRecord(meno, priez, date, id);
    }
    
     /**
     * Overí konzistenciu medzi HeapFile a LinHashFile.
     *
     * 1. Zo všetkých adries v HeapFile vytvorí množinu ID (heapIds)
     *    a pre každé ID overí, že findById(ID) v LinHashFile vráti
     *    nenull záznam s rovnakým ID.
     *
     * 2. Zo všetkých ID, ktoré LinHashFile vidí vo svojich bucket-och
     *    (getAllIdsForTesting), vytvorí množinu (hashIds) a porovná ju
     *    s heapIds. Očakáva sa, že množiny budú úplne zhodné.
     *
     * V prípade akejkoľvek nezrovnalosti vyhodí IllegalStateException
     * cez metódu fail(...). Výnimka teda pochádza z testera, nie zo
     * samotnej dátovej štruktúry.
     */
    private static void validateConsistency(HeapFile<PatientRecord> heap,
                                            LinHashFile<PatientRecord> lin) {
        // 1. ID z HeapFile
        java.util.List<Long> allAddrs = heap.getAllAddresses();
        java.util.Set<String> heapIds = new java.util.HashSet<>();

        for (long addr : allAddrs) {
            PatientRecord rec = heap.get(addr);
            if (rec == null) {
                fail("Validate: v HeapFile je na adrese " + addr + " null záznam.");
            }

            String id = rec.getId();
            if (id == null) {
                fail("Validate: záznam v HeapFile na adrese " + addr + " má null ID.");
            }

            if (!heapIds.add(id)) {
                // V testovacích generátoroch očakávame unikátne ID,
                // preto duplicitné ID považujeme za chybu implementácie
                // alebo generátora.
                fail("Validate: duplicitné ID v HeapFile: " + id);
            }

            PatientRecord fromHash = lin.findById(id);
            if (fromHash == null) {
                fail("Validate: findById(" + id + ") v LinHashFile vrátil null, "
                        + "ale záznam s týmto ID existuje v HeapFile.");
            }
            if (!id.equals(fromHash.getId())) {
                fail("Validate: findById(" + id + ") vrátil záznam s iným ID: "
                        + fromHash.getId());
            }
        }

        // 2. ID, ktoré vidí LinHashFile vo svojich bucket-och
        java.util.List<String> hashIdsList = lin.getAllIdsForTesting();
        java.util.Set<String> hashIds = new java.util.HashSet<>(hashIdsList);

        if (heapIds.size() != hashIds.size()) {
            fail("Validate: počet ID v HeapFile (" + heapIds.size()
                    + ") != počtu ID v LinHashFile (" + hashIds.size() + ").");
        }

        if (!hashIds.containsAll(heapIds)) {
            fail("Validate: LinHashFile neobsahuje všetky ID z HeapFile.");
        }

        if (!heapIds.containsAll(hashIds)) {
            fail("Validate: LinHashFile obsahuje ID, ktoré sa v HeapFile už nenachádzajú.");
        }
    }
    
    private static void deleteIfExists(String path) {
        File f = new File(path);
        if (f.exists()) {
            if (!f.delete()) {
                fail("Nepodarilo sa zmazať existujúci testovací súbor: " + path);
            }
        }
    }

    private static void assertEquals(int expected, int actual, String message) {
        if (expected != actual) {
            fail(message + " Očakávané=" + expected + ", skutočné=" + actual);
        }
    }

    private static void fail(String message) {
        throw new IllegalStateException("LinHashFileTester FAILED: " + message);
    }
    
    // voliteľné – jednoduchý main na samostatné spustenie
    public static void main(String[] args) {
        runAllTests();
    }
}
