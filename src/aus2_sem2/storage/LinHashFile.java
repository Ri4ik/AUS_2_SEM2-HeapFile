package aus2_sem2.storage;

import aus2_sem2.model.Record;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lineárne hešovanie nad existujúcim HeapFile<T>.
 *
 * - Všetky dáta (záznamy T) sú uložené v jednom HeapFile<T>.
 * - LinHashFile si v pamäti drží „adresáre“ bucket-ov (zoznam adries z HeapFile).
 * - Stav hešovania (level, nextSplit) sa ukladá do meta súboru.
 *
 * Hešujeme podľa Record.getId() – berieme jeho hashCode() a z neho počítame index
 * bucketu podľa klasického lineárneho hešovania:
 *
 *  level = i
 *  N = 2^i
 *  nextSplit = s
 *
 *  h(key) = hash(key) mod N
 *  ak h(key) < s, tak index = hash(key) mod (2N)
 */
public class LinHashFile<T extends Record> {

    /** Jedna logická „vedro“ (bucket) – zoznam adries z HeapFile. */
    private static class Bucket {
        final List<Long> addresses = new ArrayList<>();
    }

    private final HeapFile<T> heapFile;  // fyzické uloženie záznamov
    private final String metaPath;       // súbor, kde ukladáme level/nextSplit

    private int level;       // i
    private int nextSplit;   // s
    private final List<Bucket> buckets;  // adresáre bucket-ov v pamäti

    // priemerné povolené zaťaženie (záznamy na bucket) pred split-om
    private static final int SPLIT_LOAD_THRESHOLD = 8;
    // default level, ak meta súbor neexistuje
    private static final int DEFAULT_LEVEL = 1;

    /**
     * Vytvorí LinHashFile nad existujúcim HeapFile<T>.
     * @param heapFile už existujúci heap súbor so záznamami
     * @param metaPath cesta k meta súboru (napr. "patients_linhash.meta")
     */
    public LinHashFile(HeapFile<T> heapFile, String metaPath) {
        this.heapFile = heapFile;
        this.metaPath = metaPath;
        this.buckets = new ArrayList<>();

        loadMeta();                // načítame level/nextSplit alebo nastavíme defaulty
        rebuildDirectoryFromHeap(); // postavíme bucket-y podľa aktuálneho obsahu HeapFile
    }

    // ==========================
    //  Verejné API pre použitie
    // ==========================

    /**
     * Vloží záznam do lineárneho hešovacieho súboru.
     * Záznam sa fyzicky uloží do HeapFile a jeho adresa sa priradí do príslušného bucketu.
     */
    public synchronized long insert(T record) {
        if (record == null) {
            throw new IllegalArgumentException("Inserted record cannot be null.");
        }
        String id = record.getId();
        if (id == null) {
            throw new IllegalArgumentException("Record.getId() must not be null for LinHashFile.");
        }

        int bucketIndex = hashKey(id);
        ensureBucketsSize(bucketIndex + 1);

        long addr = heapFile.insert(record);
        buckets.get(bucketIndex).addresses.add(addr);

        maybeSplitAfterInsert();
        return addr;
    }

    /**
     * Vyhľadá záznam podľa ID.
     *
     * @param id hodnota kľúča (Record.getId())
     * @return nájdený záznam alebo null, ak sa nenašiel
     */
    public synchronized T findById(String id) {
        if (id == null) return null;

        int bucketIndex = hashKey(id);
        if (bucketIndex < 0 || bucketIndex >= buckets.size()) {
            return null;
        }

        Bucket b = buckets.get(bucketIndex);
        for (long addr : b.addresses) {
            T rec = heapFile.get(addr);
            if (rec == null) {
                continue; // mohol byť zmazaný
            }
            if (id.equals(rec.getId())) {
                return rec;
            }
        }
        return null;
    }

    /**
     * Zmaže prvý záznam s daným ID.
     *
     * @param id hodnota kľúča
     * @return true, ak bol záznam nájdený a zmazaný
     */
    public synchronized boolean deleteById(String id) {
        if (id == null) return false;

        int bucketIndex = hashKey(id);
        if (bucketIndex < 0 || bucketIndex >= buckets.size()) {
            return false;
        }

        Bucket b = buckets.get(bucketIndex);
        for (int i = 0; i < b.addresses.size(); i++) {
            long addr = b.addresses.get(i);
            T rec = heapFile.get(addr);
            if (rec == null) {
                // už zmazané (napr. inou operáciou) – upraceme adresáre
                b.addresses.remove(i);
                i--;
                continue;
            }
            if (id.equals(rec.getId())) {
                boolean deleted = heapFile.delete(addr);
                if (deleted) {
                    b.addresses.remove(i);
                }
                // v tejto verzii nerobíme merge (spájanie vedier), len prípadný split pri inserte
                return deleted;
            }
        }
        return false;
    }

    /**
     * Debug výpis stavu lineárneho hešovacieho súboru.
     * Vypíše level, nextSplit, počet bucket-ov a zoznam adries v jednotlivých bucket-och.
     */
    public synchronized String debugDump() {
        StringBuilder sb = new StringBuilder();
        sb.append("LinHashFile debug dump\n");
        sb.append("metaPath: ").append(metaPath).append("\n");
        sb.append("level (i): ").append(level).append("\n");
        sb.append("nextSplit (s): ").append(nextSplit).append("\n");
        sb.append("bucketCount: ").append(buckets.size()).append("\n");
        sb.append("totalValidRecords (heapFile): ").append(heapFile.getTotalValidRecords()).append("\n");
        sb.append("\n");

        for (int i = 0; i < buckets.size(); i++) {
            Bucket b = buckets.get(i);
            sb.append("Bucket ").append(i)
              .append(" [size=").append(b.addresses.size()).append("]\n");
            for (long addr : b.addresses) {
                T rec = heapFile.get(addr);
                sb.append("  addr=").append(addr).append(" : ");
                if (rec != null) {
                    sb.append(rec.toString());
                } else {
                    sb.append("<deleted>");
                }
                sb.append("\n");
            }
            sb.append("\n");
        }

        return sb.toString();
    }

    /**
     * Korektné ukončenie práce – uloží meta informácie a zavrie HeapFile.
     */
    public synchronized void close() {
        saveMeta();
        heapFile.close();
    }

    // ==========================
    //  Interné pomocné metódy
    // ==========================

    /**
     * Načíta level a nextSplit z meta súboru.
     * Ak súbor neexistuje alebo je chybný, nastaví default hodnoty.
     */
    private void loadMeta() {
        File f = new File(metaPath);
        if (!f.exists()) {
            // nová DB – začneme od level = 1, nextSplit = 0
            this.level = DEFAULT_LEVEL;
            this.nextSplit = 0;
            return;
        }

        try (DataInputStream dis = new DataInputStream(new FileInputStream(f))) {
            this.level = dis.readInt();
            this.nextSplit = dis.readInt();

            if (level < 0) level = DEFAULT_LEVEL;
            if (nextSplit < 0) nextSplit = 0;
        } catch (IOException e) {
            // pri chybe nastavíme default
            this.level = DEFAULT_LEVEL;
            this.nextSplit = 0;
        }
    }

    /**
     * Uloží level a nextSplit do meta súboru.
     */
    private void saveMeta() {
        File f = new File(metaPath);
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(f))) {
            dos.writeInt(level);
            dos.writeInt(nextSplit);
        } catch (IOException e) {
            // meta nie je kritická pre beh, chybu môžeme ignorovať
        }
    }

    /**
     * Znovu postaví bucket-y podľa aktuálneho obsahu HeapFile.
     * Načítame všetky adresy a každú z nich priradíme do bucketu podľa hashKey().
     * Volá sa v konštruktore po loadMeta().
     */
    private void rebuildDirectoryFromHeap() {
        buckets.clear();

        int bucketCount = getCurrentBucketCount();
        for (int i = 0; i < bucketCount; i++) {
            buckets.add(new Bucket());
        }

        List<Long> allAddrs = heapFile.getAllAddresses();
        for (long addr : allAddrs) {
            T rec = heapFile.get(addr);
            if (rec == null) {
                continue;
            }
            String id = rec.getId();
            if (id == null) {
                continue;
            }
            int bucketIndex = hashKey(id);
            ensureBucketsSize(bucketIndex + 1);
            buckets.get(bucketIndex).addresses.add(addr);
        }
    }

    /** Vráti aktuálny počet bucket-ov: 2^level + nextSplit. */
    private int getCurrentBucketCount() {
        int base = 1 << level; // 2^i
        return base + nextSplit;
    }

    /** Zaistí, že zoznam bucket-ov má aspoň danú veľkosť. */
    private void ensureBucketsSize(int size) {
        while (buckets.size() < size) {
            buckets.add(new Bucket());
        }
    }

    /** Urobí hash z kľúča (String) na nezáporné int. */
    private int positiveHash(String key) {
        int h = key.hashCode();
        if (h < 0) {
            // pozor na Integer.MIN_VALUE – abs by mohol znova dať záporné
            if (h == Integer.MIN_VALUE) {
                h = 0;
            } else {
                h = -h;
            }
        }
        return h;
    }

    /**
     * Lineárny hash pre daný kľúč:
     *
     * N = 2^level
     * idx = h % N
     * ak idx < nextSplit, tak idx = h % (2N)
     */
    private int hashKey(String key) {
        int h = positiveHash(key);
        int base = 1 << level;   // N
        int idx = h % base;

        if (idx < nextSplit) {
            idx = h % (base << 1); // 2N
        }

        return idx;
    }

    /**
     * Po inserte skontroluje priemerné zaťaženie a ak je príliš vysoké,
     * urobí split nasledujúceho bucketu (bucket index = nextSplit).
     */
    private void maybeSplitAfterInsert() {
        int totalRecords = heapFile.getTotalValidRecords();
        int bucketCount = getCurrentBucketCount();
        if (bucketCount == 0) return;

        int avgLoad = totalRecords / bucketCount;
        if (avgLoad > SPLIT_LOAD_THRESHOLD) {
            splitNextBucket();
        }
    }

    /**
     * Urobí split bucketu s indexom nextSplit podľa algoritmu lineárneho hešovania.
     *
     * Algoritmus:
     *  - N = 2^level
     *  - bucketToSplit = nextSplit
     *  - newBucketIndex = bucketToSplit + N
     *  - všetky záznamy v bucketToSplit prehešujeme modulo 2N
     *    a rozdelíme medzi bucketToSplit a newBucketIndex
     *  - nextSplit++
     *  - ak nextSplit == N, tak nextSplit = 0 a level++
     */
    private void splitNextBucket() {
        int N = 1 << level;      // základný počet bucketov na danom levele
        int bucketToSplit = nextSplit;
        int newBucketIndex = bucketToSplit + N;

        ensureBucketsSize(newBucketIndex + 1);

        Bucket oldBucket = buckets.get(bucketToSplit);
        Bucket newBucket = buckets.get(newBucketIndex);
        newBucket.addresses.clear();

        // skopírujeme zoznam adries, pôvodný vyčistíme
        List<Long> oldAddresses = new ArrayList<>(oldBucket.addresses);
        oldBucket.addresses.clear();

        for (long addr : oldAddresses) {
            T rec = heapFile.get(addr);
            if (rec == null) {
                // záznam mohol byť medzitým zmazaný
                continue;
            }
            String id = rec.getId();
            if (id == null) {
                // bez kľúča – teoreticky by sa v LinHashFile nemal objaviť
                continue;
            }

            int h = positiveHash(id);
            int idx2 = h % (N << 1); // mod 2N

            if (idx2 == bucketToSplit) {
                oldBucket.addresses.add(addr);
            } else if (idx2 == newBucketIndex) {
                newBucket.addresses.add(addr);
            } else {
                // v lineárnom hešovaní by sa mal výsledok rozdeliť presne na tieto dve hodnoty,
                // ale pre istotu fallback na globálny hash:
                int realIdx = hashKey(id);
                ensureBucketsSize(realIdx + 1);
                buckets.get(realIdx).addresses.add(addr);
            }
        }

        // posunieme nextSplit a prípadne zvýšime level
        nextSplit++;
        if (nextSplit == N) {
            nextSplit = 0;
            level++;
        }

        saveMeta();
    }
    
     /**
     * Vráti zoznam všetkých ID, ktoré sa aktuálne nachádzajú
     * v bucket-och lineárneho hešovania.
     *
     * Táto metóda je určená výhradne na testovanie konzistencie
     * v testeroch (napr. LinHashFileTester). Nevyhadzuje žiadne
     * výnimky, iba číta dáta z HeapFile a bucket-ov.
     */
    public synchronized java.util.List<String> getAllIdsForTesting() {
        java.util.List<String> ids = new java.util.ArrayList<>();

        for (int i = 0; i < buckets.size(); i++) {
            Bucket b = buckets.get(i);
            for (long addr : b.addresses) {
                T rec = heapFile.get(addr);
                if (rec == null) {
                    // záznam už môže byť zmazaný, tento prípad iba ignorujeme
                    continue;
                }
                String id = rec.getId();
                if (id == null) {
                    // záznam bez ID nás pri testovaní nezaujíma
                    continue;
                }
                ids.add(id);
            }
        }

        return ids;
    }
}
