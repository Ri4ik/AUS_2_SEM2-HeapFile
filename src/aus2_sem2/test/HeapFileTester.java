package aus2_sem2.test;

import aus2_sem2.model.PatientRecord;
import aus2_sem2.storage.HeapFile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Tester pre HeapFile<PatientRecord>.
 *
 * - Vytvorí samostatný testovací súbor (neovplyvní produkčné dáta).
 * - Vloží 10000 záznamov.
 * - Otestuje 2000 náhodných/sekvenčných čítaní.
 * - Zmaže 5000 záznamov.
 * - Overí všetky základné invarianty (počty, get, getAllAddresses, existsId, reopen).
 *
 * Pri akejkoľvek nezrovnalosti vyhodí IllegalStateException z testera 
 */
public class HeapFileTester {

    // počet testovacích operácií
    private static final int INSERT_COUNT = 1_000;
    private static final int FIND_COUNT   = 1_000;
    private static final int DELETE_COUNT = 1_000;

    /**
     * Spustí celý testovací scenár pre HeapFile.
     * V prípade chyby vyhodí IllegalStateException s popisom problému.
     */
    public static void runAllTests() {
        String testFilePath = "heapfile_patients_test.dat";

        // vždy začneme s čistým súborom
        deleteIfExists(testFilePath);

        HeapFile<PatientRecord> heap = new HeapFile<>(testFilePath, 256, PatientRecord.class);

        try {
            System.out.println("=== HeapFileTester: START ===");
            testInsertAndGet(heap);
            testDelete(heap);
            testReopen(heap, testFilePath);
            System.out.println("=== HeapFileTester: OK ===");
        } finally {
            // korektne zatvoriť súbor
            heap.close();
        }
    }

    // =======================
    // 1. Vkladanie + čítanie
    // =======================

    private static void testInsertAndGet(HeapFile<PatientRecord> heap) {
        System.out.println("[HeapFileTester] Insert " + INSERT_COUNT + " records...");

        List<PatientRecord> records = new ArrayList<>(INSERT_COUNT);
        List<Long> addresses = new ArrayList<>(INSERT_COUNT);

        for (int i = 0; i < INSERT_COUNT; i++) {
            PatientRecord rec = generateRecord(i);
            long addr = heap.insert(rec);

            if (addr < 0) {
                fail("Insert returned negative address for record " + i);
            }

            records.add(rec);
            addresses.add(addr);
        }

        // overenie počtu
        int total = heap.getTotalValidRecords();
        assertEquals(INSERT_COUNT, total,
                "Počet platných záznamov po vkladaní je nesprávny.");

        List<Long> allAddrs = heap.getAllAddresses();
        assertEquals(INSERT_COUNT, allAddrs.size(),
                "getAllAddresses() nevrátil správny počet adries po vkladaní.");
        
        // otestujeme 2000 čítaní (sekvenčne z prvých N)
         if (FIND_COUNT > INSERT_COUNT) {
            fail("FIND_COUNT (" + FIND_COUNT + ") je väčší ako INSERT_COUNT (" + INSERT_COUNT + "). "
                    + "Tester je zle nakonfigurovaný.");
        }
        System.out.println("[HeapFileTester] Get " + FIND_COUNT + " records...");
//        int limit = Math.min(FIND_COUNT, INSERT_COUNT);
        for (int i = 0; i < FIND_COUNT; i++) {
            long addr = addresses.get(i);
            PatientRecord fromHeap = heap.get(addr);
            if (fromHeap == null) {
                fail("Get vrátil null pre adresu " + addr + " po vkladaní.");
            }

            PatientRecord original = records.get(i);
            if (!original.getId().equals(fromHeap.getId())) {
                fail("Nesúlad ID pri čítaní: očakávané " +
                        original.getId() + ", dostal som " + fromHeap.getId());
            }

            // overíme aj existsId
            if (!heap.existsId(original.getId())) {
                fail("existsId(" + original.getId() + ") vrátil false, ale záznam existuje.");
            }
        }

        System.out.println("[HeapFileTester] Insert/Get OK");
    }

    // =======================
    // 2. Mazanie
    // =======================

    private static void testDelete(HeapFile<PatientRecord> heap) {
        System.out.println("[HeapFileTester] Delete " + DELETE_COUNT + " records...");

        // získame adresy a ID
        List<Long> allAddrs = heap.getAllAddresses();
        int totalBefore = heap.getTotalValidRecords();

        if (allAddrs.size() != totalBefore) {
            fail("Pred mazaním: getAllAddresses().size() != totalValidRecords().");
        }

        if (DELETE_COUNT > totalBefore) {
            fail("DELETE_COUNT je väčší než počet existujúcich záznamov.");
        }

        List<Long> toDelete = new ArrayList<>(DELETE_COUNT);
        List<String> deleteIds = new ArrayList<>(DELETE_COUNT);
        List<Long> toKeep = new ArrayList<>(totalBefore - DELETE_COUNT);
        List<String> keepIds = new ArrayList<>(totalBefore - DELETE_COUNT);

        // pre jednoduchosť: zmažeme prvých DELETE_COUNT záznamov
        for (int i = 0; i < allAddrs.size(); i++) {
            long addr = allAddrs.get(i);
            PatientRecord rec = heap.get(addr);
            if (rec == null) {
                fail("Zaznam na adrese " + addr + " je null ešte pred mazaním.");
            }
            if (i < DELETE_COUNT) {
                toDelete.add(addr);
                deleteIds.add(rec.getId());
            } else {
                toKeep.add(addr);
                keepIds.add(rec.getId());
            }
        }

        // fyzické mazanie
        for (int i = 0; i < DELETE_COUNT; i++) {
            long addr = toDelete.get(i);
            boolean deleted = heap.delete(addr);
            if (!deleted) {
                fail("Mazanie záznamu na adrese " + addr + " zlyhalo.");
            }
        }

        int totalAfter = heap.getTotalValidRecords();
        int expectedAfter = totalBefore - DELETE_COUNT;
        assertEquals(expectedAfter, totalAfter,
                "Počet platných záznamov po mazaní je nesprávny.");

        // kontrola, že zmazané záznamy sú naozaj preč
        for (int i = 0; i < DELETE_COUNT; i++) {
            long addr = toDelete.get(i);
            String id = deleteIds.get(i);

            PatientRecord rec = heap.get(addr);
            if (rec != null) {
                fail("Po mazaní je na adrese " + addr +
                        " stále neprázdny záznam: " + rec);
            }
            if (heap.existsId(id)) {
                fail("existsId(" + id + ") po mazaní vrátil true.");
            }
        }

        // kontrola, že nezmazané záznamy sú stále dostupné
        for (int i = 0; i < toKeep.size(); i++) {
            long addr = toKeep.get(i);
            String id = keepIds.get(i);

            PatientRecord rec = heap.get(addr);
            if (rec == null) {
                fail("Záznam, ktorý sme nemažali, je null na adrese " + addr);
            } else if (!id.equals(rec.getId())) {
                fail("Záznam na adrese " + addr +
                        " má iné ID než očakávané: " + rec.getId() + " vs " + id);
            }

            if (!heap.existsId(id)) {
                fail("existsId(" + id + ") po mazaní vrátil false, ale záznam ešte existuje.");
            }
        }

        // kontrola konzistencie getAllAddresses
        List<Long> addressesAfter = heap.getAllAddresses();
        assertEquals(expectedAfter, addressesAfter.size(),
                "getAllAddresses().size() po mazaní nesedí s totalValidRecords().");

        System.out.println("[HeapFileTester] Delete OK");
    }

    // =======================
    // 3. Reopen test
    // =======================

    private static void testReopen(HeapFile<PatientRecord> heap, String path) {
        System.out.println("[HeapFileTester] Reopen test...");

        int totalBeforeClose = heap.getTotalValidRecords();
        List<Long> addressesBefore = heap.getAllAddresses();

        // zatvoriť starý
        heap.close();

        // znovu otvoriť ten istý súbor
        HeapFile<PatientRecord> reopened =
                new HeapFile<>(path, 256, PatientRecord.class);

        try {
            int totalAfter = reopened.getTotalValidRecords();
            assertEquals(totalBeforeClose, totalAfter,
                    "Po opätovnom otvorení súboru nesedí totalValidRecords().");

            List<Long> addressesAfter = reopened.getAllAddresses();
            assertEquals(addressesBefore.size(), addressesAfter.size(),
                    "Po opätovnom otvorení súboru nesedí počet adries.");

            System.out.println("[HeapFileTester] Reopen OK");
        } finally {
            reopened.close();
        }
    }

    // =======================
    // Pomocné metódy
    // =======================

    private static PatientRecord generateRecord(int i) {
        String meno = "TestM" + i;
        String priez = "TestP" + i;
        String date = String.format("%02d:%02d:%04d",
                (i % 28) + 1,
                (i % 12) + 1,
                1970 + (i % 50));
        String id = String.format("T%07d", i);
        return new PatientRecord(meno, priez, date, id);
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
        throw new IllegalStateException("HeapFileTester FAILED: " + message);
    }

    // voliteľné – jednoduchý main na samostatné spustenie
    public static void main(String[] args) {
        runAllTests();
    }
}
