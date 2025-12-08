package aus2_sem2.test;

import aus2_sem2.model.PatientRecord;
import aus2_sem2.storage.Block;
import aus2_sem2.storage.HeapFile;
import aus2_sem2.storage.LinHashFile;

import java.io.File;
import java.util.*;

public class LinHashFileTester {

    private static final int INSERT_COUNT = 5000;
    private static final int FIND_COUNT   = 1500;
    private static final int DELETE_COUNT = 1500;

    public static void runAllTests() {
        String base = "linhash_test";

        deleteIfExists(base + "_lh_primary.dat");
        deleteIfExists(base + "_lh_overflow.dat");
        deleteIfExists(base + "_lhmeta.dat");

        try {
            LinHashFile<PatientRecord> lin = new LinHashFile<>(
                    base,
                    256,
                    PatientRecord.class,
                    4,
                    0.75,
                    0.40
            );

            System.out.println("=== LinHashFileTester: START ===");
            testInsertAndFind(lin);
            testDelete(lin);
            testReopen(base);
            System.out.println("=== LinHashFileTester: OK ===");

            lin.close();
        } catch (Exception e) {
            throw new IllegalStateException("LinHashFileTester FAILED: " + e.getMessage(), e);
        }
    }

    // =============================
    // 1. INSERT + FIND
    // =============================
    private static void testInsertAndFind(LinHashFile<PatientRecord> lin) throws Exception {
        System.out.println("[Tester] Insert " + INSERT_COUNT);

        List<String> ids = new ArrayList<>();

        for (int i = 0; i < INSERT_COUNT; i++) {
            PatientRecord r = generateRecord(i);
            lin.insert(r);
            ids.add(r.getId());
        }

        if (lin.getTotalRecords() != INSERT_COUNT) {
            fail("TotalRecords != INSERT_COUNT");
        }

        System.out.println("[Tester] Find " + FIND_COUNT);
        for (int i = 0; i < FIND_COUNT; i++) {
            String id = ids.get(i);
            PatientRecord found = lin.findById(id);
            if (found == null) fail("findById returned null for " + id);
            if (!id.equals(found.getId()))
                fail("Wrong ID returned by findById: " + found.getId());
        }

        validateConsistency(lin);
        System.out.println("[Tester] Insert/Find OK");
    }

    // =============================
    // 2. DELETE
    // =============================
    private static void testDelete(LinHashFile<PatientRecord> lin) throws Exception {
        System.out.println("[Tester] Delete " + DELETE_COUNT);

        // собираем ВСЕ ID из структуры
        List<String> all = getAllIds(lin);

        int before = all.size();
        if (before < DELETE_COUNT) fail("Too few records before delete");

        List<String> toDelete = all.subList(0, DELETE_COUNT);
        List<String> toKeep = all.subList(DELETE_COUNT, all.size());

        // delete
        for (String id : toDelete) {
            if (!lin.deleteById(id)) {
                fail("deleteById returned false for " + id);
            }
        }

        if (lin.getTotalRecords() != before - DELETE_COUNT) {
            fail("TotalRecords mismatch after delete");
        }

        // deleted must NOT be found
        for (String id : toDelete) {
            if (lin.findById(id) != null) {
                fail("ID still exists after delete: " + id);
            }
        }

        // kept must still exist
        for (String id : toKeep) {
            PatientRecord r = lin.findById(id);
            if (r == null) fail("Kept ID disappeared: " + id);
        }

        validateConsistency(lin);
        System.out.println("[Tester] Delete OK");
    }

    // =============================
    // 3. REOPEN
    // =============================
    private static void testReopen(String base) throws Exception {
        System.out.println("[Tester] Reopen...");

        LinHashFile<PatientRecord> lin = new LinHashFile<>(
                base,
                256,
                PatientRecord.class,
                4,
                0.75,
                0.40
        );

        List<String> ids = getAllIds(lin);

        for (String id : ids) {
            PatientRecord r = lin.findById(id);
            if (r == null) fail("After reopen: ID not found: " + id);
            if (!id.equals(r.getId())) fail("After reopen: wrong ID returned");
        }

        validateConsistency(lin);
        lin.close();
        System.out.println("[Tester] Reopen OK");
    }

    // =============================
    // CONSISTENCY CHECK
    // =============================
    private static void validateConsistency(LinHashFile<PatientRecord> lin) throws Exception {
        HeapFile<PatientRecord> primary = lin.getPrimaryFile();
        HeapFile<PatientRecord> overflow = lin.getOverflowFile();

        Set<String> heapIds = new HashSet<>();
        collectIdsFromHeap(primary, heapIds);
        collectIdsFromHeap(overflow, heapIds);

        if (heapIds.size() != lin.getTotalRecords()) {
            fail("HeapFile count != totalRecords");
        }

        for (String id : heapIds) {
            PatientRecord r = lin.findById(id);
            if (r == null) fail("Inconsistency: findById returned null for " + id);
        }
    }

    private static void collectIdsFromHeap(HeapFile<PatientRecord> heap, Set<String> out)
            throws Exception {
        List<Long> addrs = heap.getAllAddresses();
        for (long addr : addrs) {
            PatientRecord r = heap.get(addr);
            if (r == null) fail("Null record in heap at addr=" + addr);
            out.add(r.getId());
        }
    }

    // =============================
    // HELPERS
    // =============================
    private static List<String> getAllIds(LinHashFile<PatientRecord> lin) throws Exception {
        Set<String> out = new HashSet<>();
        collectIdsFromHeap(lin.getPrimaryFile(), out);
        collectIdsFromHeap(lin.getOverflowFile(), out);
        return new ArrayList<>(out);
    }

    private static PatientRecord generateRecord(int i) {
        return new PatientRecord(
                "LH_M" + i,
                "LH_P" + i,
                "01:01:2000",
                String.format("LH%07d", i)
        );
    }

    private static void deleteIfExists(String path) {
        File f = new File(path);
        if (f.exists()) f.delete();
    }

    private static void fail(String msg) {
        throw new IllegalStateException("LinHashFileTester FAILED: " + msg);
    }

    public static void main(String[] args) {
        runAllTests();
    }
}
