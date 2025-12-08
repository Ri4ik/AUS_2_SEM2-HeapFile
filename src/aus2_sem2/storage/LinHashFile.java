package aus2_sem2.storage;

import aus2_sem2.model.Record;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Lineárne hešovanie nad dvomi HeapFile súbormi:
 *  - primaryFile  (bucket-y, každý bucket = primárny blok skupiny),
 *  - overflowFile (overflow bloky pre dlhé reťazce).
 *
 * Kľúč = Record.getId() (String).
 *
 * Meta-informácie (M, level, splitPtr, atď.) sa uchovávajú v samostatnom súbore
 * s príponou "_lhmeta.dat".
 */
public class LinHashFile<T extends Record> implements Closeable {

    // ===== konštanty =====

    private static final int META_MAGIC   = 0x4C483231;  // "LH21"
    private static final int META_VERSION = 1;

    /** Počiatočný počet skupín M. */
    private final int initialGroupCount;

    /** Horný prah hustoty – pri prekročení vykonáme split. */
    private final double dMax;
    /** Dolný prah hustoty – pri poklese môžeme spraviť merge. */
    private final double dMin;

    /** Trieda záznamu (Record subclass). */
    private final Class<T> recordClass;

    // ===== súbory =====

    private final HeapFile<T> primaryFile;
    private final HeapFile<T> overflowFile;
    private final String metaPath;

    // ===== meta parametre lineárneho hešovania =====

    /** Aktuálna úroveň u. */
    private int level;
    /** Ukazovateľ S – index ďalšej skupiny na split. */
    private int splitPtr;
    /** Počet skupín (bucketov). */
    private int groupCount;
    /** Počet platných záznamov v celej štruktúre. */
    private long totalRecords;

    /** Pre každú skupinu → index primárneho bloku v primaryFile. */
    private final List<Integer> primaryBlockIndexByGroup = new ArrayList<>();
    /** Pre každú skupinu → index prvého overflow bloku (-1 ak žiadny). */
    private final List<Integer> firstOverflowBlockIndexByGroup = new ArrayList<>();

    /**
     * overflowNext[i] = index nasledujúceho overflow bloku,
     * alebo -1, ak je i posledný v reťazci.
     */
    private final List<Integer> overflowNext = new ArrayList<>();

    // ===== konštruktor =====

    /**
     * @param basePath základná cesta bez prípony (napr. "pcr_index").
     *                 Budú použité:
     *                   basePath + "_lh_primary.dat"
     *                   basePath + "_lh_overflow.dat"
     *                   basePath + "_lhmeta.dat"
     * @param clusterSizeBytes veľkosť bloku (rovnaká ako pre HeapFile)
     * @param recordClass trieda záznamu
     * @param initialGroupCount počiatočný M (napr. 2 alebo 4)
     * @param dMax horný prah hustoty (typicky 0.75 – 0.85)
     * @param dMin dolný prah hustoty (napr. 0.4 – 0.5)
     */
    public LinHashFile(String basePath,
                       int clusterSizeBytes,
                       Class<T> recordClass,
                       int initialGroupCount,
                       double dMax,
                       double dMin) throws IOException {

        this.initialGroupCount = initialGroupCount;
        this.dMax = dMax;
        this.dMin = dMin;
        this.recordClass = recordClass;

        String primaryPath  = basePath + "_lh_primary.dat";
        String overflowPath = basePath + "_lh_overflow.dat";
        this.metaPath       = basePath + "_lhmeta.dat";

        this.primaryFile  = new HeapFile<>(primaryPath,  clusterSizeBytes, recordClass);
        this.overflowFile = new HeapFile<>(overflowPath, clusterSizeBytes, recordClass);

        File metaFile = new File(metaPath);
        if (metaFile.exists() && metaFile.length() > 0) {
            loadMeta();
        } else {
            initNewStructure();
            saveMeta();
        }
    }

    // ===== základ info =====

    public long getTotalRecords() {
        return totalRecords;
    }

    public int getLevel() {
        return level;
    }

    public int getSplitPtr() {
        return splitPtr;
    }

    public int getGroupCount() {
        return groupCount;
    }

    public HeapFile<T> getPrimaryFile() {
        return primaryFile;
    }

    public HeapFile<T> getOverflowFile() {
        return overflowFile;
    }

    // ===== inicializácia novej štruktúry =====

    /**
     * Inicializuje úplne nové lineárne hešovanie – M prázdnych skupín,
     * každá s vlastným primárnym blokom.
     */
    private void initNewStructure() throws IOException {
        this.level        = 0;
        this.splitPtr     = 0;
        this.totalRecords = 0;
        this.groupCount   = initialGroupCount;

        primaryBlockIndexByGroup.clear();
        firstOverflowBlockIndexByGroup.clear();
        overflowNext.clear();

        // vytvoríme M prázdnych primárnych blokov
        for (int g = 0; g < initialGroupCount; g++) {
            int blockIndex = primaryFile.allocateEmptyBlockOrReuse();
            primaryBlockIndexByGroup.add(blockIndex);
            firstOverflowBlockIndexByGroup.add(-1);
        }

        // overflow súbor môže byť na začiatku prázdny
        int ovCount = overflowFile.getBlockCount();
        for (int i = 0; i < ovCount; i++) {
            overflowNext.add(-1);
        }
    }

    // ===== hash + výpočet skupiny =====

    /** Zabezpečí nezáporný hash. */
    private int positiveHash(String key) {
        return key.hashCode() & 0x7fffffff;
    }

    /** Základný počet skupín B = M * 2^level. */
    private int baseGroupCount() {
        return initialGroupCount * (1 << level);
    }

    /**
     * Podľa algoritmu lineárneho hešovania:
     *  i = h(k) mod B
     *  ak i < S → i = h(k) mod (2B)
     */
    private int computeGroupIndex(String id) {
        int h = positiveHash(id);
        int B = baseGroupCount();
        int i = h % B;
        if (i < 0) i += B;

        if (i < splitPtr) {
            int B2 = B * 2;
            i = h % B2;
            if (i < 0) i += B2;
        }
        return i;
    }

    // ===== verejné operácie: INSERT / FIND / DELETE / EDIT =====

    /**
     * Vloží záznam (bez kontroly unikátnosti ID).
     */
    public synchronized void insert(T record) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException("record == null");
        }
        String id = record.getId();
        if (id == null) {
            throw new IllegalArgumentException("Record.getId() == null");
        }

        int group = computeGroupIndex(id);
        insertIntoGroup(group, record);
        totalRecords++;

        trySplitIfNeeded();
        saveMeta();
    }

    /**
     * Vyhľadá záznam podľa ID.
     */
    public synchronized T findById(String id) throws IOException {
        if (id == null || id.isEmpty()) {
            return null;
        }
        int group = computeGroupIndex(id);
        if (group < 0 || group >= groupCount) {
            return null;
        }

        int primaryIdx = primaryBlockIndexByGroup.get(group);
        Block<T> primaryBlock = primaryFile.readBlock(primaryIdx);

        T inPrimary = primaryBlock.findById(id);
        if (inPrimary != null) {
            return inPrimary;
        }

        int firstOv = firstOverflowBlockIndexByGroup.get(group);
        int current = firstOv;
        while (current != -1) {
            Block<T> ovBlock = overflowFile.readBlock(current);
            T r = ovBlock.findById(id);
            if (r != null) {
                return r;
            }
            current = getOverflowNext(current);
        }

        return null;
    }

    /**
     * Editácia – nájde záznam podľa ID a nahradí obsah bajtami z updated.
     * Predpoklad: ID sa nemení.
     */
    public synchronized boolean editById(T updated) throws IOException {
        if (updated == null) return false;
        String id = updated.getId();
        if (id == null) return false;

        int group = computeGroupIndex(id);
        if (group < 0 || group >= groupCount) return false;

        byte[] newBytes = updated.toByteArray();

        int primaryIdx = primaryBlockIndexByGroup.get(group);
        Block<T> primaryBlock = primaryFile.readBlock(primaryIdx);
        for (int i = 0; i < primaryBlock.getCapacity(); i++) {
            T r = primaryBlock.getRecord(i);
            if (r != null && id.equals(r.getId())) {
                r.fromByteArray(newBytes);
                primaryFile.writeBlock(primaryIdx, primaryBlock);
                return true;
            }
        }

        int current = firstOverflowBlockIndexByGroup.get(group);
        while (current != -1) {
            Block<T> ovBlock = overflowFile.readBlock(current);
            for (int i = 0; i < ovBlock.getCapacity(); i++) {
                T r = ovBlock.getRecord(i);
                if (r != null && id.equals(r.getId())) {
                    r.fromByteArray(newBytes);
                    overflowFile.writeBlock(current, ovBlock);
                    return true;
                }
            }
            current = getOverflowNext(current);
        }

        return false;
    }

    /**
     * Zmaže záznam podľa ID.
     * Po úspešnom mazaní:
     *  - pokus o „striasanie“ overflow reťazca (ak to vie uvoľniť bloky),
     *  - overflowFile.shrinkEmptyTailBlocks(),
     *  - prípadný merge podľa dMin.
     */
    public synchronized boolean deleteById(String id) throws IOException {
        if (id == null || id.isEmpty()) {
            return false;
        }

        int group = computeGroupIndex(id);
        if (group < 0 || group >= groupCount) {
            return false;
        }

        boolean removed = deleteFromGroup(group, id);
        if (!removed) {
            return false;
        }

        totalRecords--;

        boolean freedBlocks = tryCompactOverflowAfterDelete(group);
        if (freedBlocks) {
            overflowFile.shrinkEmptyTailBlocks();
        }

        tryMergeIfNeeded();
        saveMeta();
        return true;
    }

    // ===== práca s konkrétnou skupinou =====

    /** Uistí sa, že group index existuje (doplnenie skupín pri splite). */
    private void ensureGroupExists(int group) throws IOException {
        while (group >= groupCount) {
            int newPrimaryIdx = primaryFile.allocateEmptyBlockOrReuse();
            primaryBlockIndexByGroup.add(newPrimaryIdx);
            firstOverflowBlockIndexByGroup.add(-1);
            groupCount++;
        }
    }

    /** Ošetrí overflowNext.size tak, aby index existoval. */
    private void ensureOverflowIndexExists(int index) {
        while (overflowNext.size() <= index) {
            overflowNext.add(-1);
        }
    }

    private int getOverflowNext(int index) {
        if (index < 0 || index >= overflowNext.size()) return -1;
        Integer v = overflowNext.get(index);
        return (v == null) ? -1 : v;
    }

    private void setOverflowNext(int index, int nextIndex) {
        ensureOverflowIndexExists(index);
        overflowNext.set(index, nextIndex);
    }

    /**
     * Vloží záznam do danej skupiny (primárny blok + overflow reťazec).
     */
    private void insertIntoGroup(int group, T record) throws IOException {
        ensureGroupExists(group);

        int primaryIdx = primaryBlockIndexByGroup.get(group);
        Block<T> primaryBlock = primaryFile.readBlock(primaryIdx);

        // 1) primárny blok
        if (!primaryBlock.isFull()) {
            primaryBlock.insert(record);
            primaryFile.writeBlock(primaryIdx, primaryBlock);
            return;
        }

        // 2) overflow reťazec
        int firstOv = firstOverflowBlockIndexByGroup.get(group);
        if (firstOv == -1) {
            int newIdx = overflowFile.allocateEmptyBlockOrReuse();
            Block<T> newOv = overflowFile.readBlock(newIdx);
            newOv.insert(record);
            overflowFile.writeBlock(newIdx, newOv);

            firstOverflowBlockIndexByGroup.set(group, newIdx);
            setOverflowNext(newIdx, -1);
            return;
        }

        int current = firstOv;
        while (true) {
            Block<T> ovBlock = overflowFile.readBlock(current);
            if (!ovBlock.isFull()) {
                ovBlock.insert(record);
                overflowFile.writeBlock(current, ovBlock);
                return;
            }
            int next = getOverflowNext(current);
            if (next == -1) {
                int newIdx = overflowFile.allocateEmptyBlockOrReuse();
                Block<T> newOv = overflowFile.readBlock(newIdx);
                newOv.insert(record);
                overflowFile.writeBlock(newIdx, newOv);

                setOverflowNext(current, newIdx);
                setOverflowNext(newIdx, -1);
                return;
            } else {
                current = next;
            }
        }
    }

    /** Nájde a zmaže záznam s ID v skupine. */
    private boolean deleteFromGroup(int group, String id) throws IOException {
        int primaryIdx = primaryBlockIndexByGroup.get(group);
        Block<T> primaryBlock = primaryFile.readBlock(primaryIdx);

        // 1) primárny blok
        boolean removed = primaryBlock.deleteById(id);
        if (removed) {
            primaryFile.writeBlock(primaryIdx, primaryBlock);
            return true;
        }

        // 2) overflow
        int firstOv = firstOverflowBlockIndexByGroup.get(group);
        int current = firstOv;
        int prev = -1;

        while (current != -1) {
            Block<T> ovBlock = overflowFile.readBlock(current);
            boolean removedInOv = ovBlock.deleteById(id);
            if (removedInOv) {
                overflowFile.writeBlock(current, ovBlock);

                if (ovBlock.isEmpty()) {
                    int next = getOverflowNext(current);
                    if (prev == -1) {
                        firstOverflowBlockIndexByGroup.set(group, next);
                    } else {
                        setOverflowNext(prev, next);
                    }
                    setOverflowNext(current, -1);
                }
                return true;
            }

            prev = current;
            current = getOverflowNext(current);
        }

        return false;
    }

    // ===== striasanie overflow reťazca po mazaní =====

    /**
     * Striasanie overflow reťazca danej skupiny.
     *
     * Podmienka:
     *  - skutočne meníme usporiadanie blokov iba vtedy,
     *    ak vieme zmenšiť počet overflow blokov tejto skupiny
     *    (t.j. uvoľniť aspoň jeden blok).
     *
     * @return true ak sa pri tom uvoľnili nejaké overflow bloky.
     */
    private boolean tryCompactOverflowAfterDelete(int group) throws IOException {
        if (group < 0 || group >= groupCount) {
            return false;
        }

        int firstOv = firstOverflowBlockIndexByGroup.get(group);
        if (firstOv == -1) {
            return false;    // žiadny overflow reťazec
        }

        int primaryIdx = primaryBlockIndexByGroup.get(group);

        // 1) nazbierame indexy všetkých overflow blokov v reťazci
        List<Integer> chain = new ArrayList<>();
        int current = firstOv;
        while (current != -1) {
            chain.add(current);
            current = getOverflowNext(current);
        }
        if (chain.isEmpty()) {
            firstOverflowBlockIndexByGroup.set(group, -1);
            return false;
        }

        // 2) nazbierame všetky platné záznamy z primárneho + overflow blokov
        List<T> records = new ArrayList<>();

        Block<T> primaryBlock = primaryFile.readBlock(primaryIdx);
        for (int i = 0; i < primaryBlock.getCapacity(); i++) {
            T r = primaryBlock.getRecord(i);
            if (r != null) {
                records.add(r);
            }
        }

        for (int idx : chain) {
            Block<T> ovBlock = overflowFile.readBlock(idx);
            for (int i = 0; i < ovBlock.getCapacity(); i++) {
                T r = ovBlock.getRecord(i);
                if (r != null) {
                    records.add(r);
                }
            }
        }

        int k          = chain.size();
        int totalLocal = records.size();

        int primaryCap  = primaryBlock.getCapacity();
        int overflowCap = overflowFile.getRecordsPerBlock();
        int recSize     = primaryBlock.getRecordSize();

        // špeciálny prípad – v skupine nezostal žiadny záznam
        if (totalLocal == 0) {
            Block<T> emptyPrimary = new Block<>(primaryIdx,
                    primaryCap,
                    recSize,
                    recordClass, true);
            primaryFile.writeBlock(primaryIdx, emptyPrimary);

            for (int idx : chain) {
                Block<T> emptyOv = new Block<>(idx,
                        overflowCap,
                        recSize,
                        recordClass, true);
                overflowFile.writeBlock(idx, emptyOv);
                setOverflowNext(idx, -1);
            }
            firstOverflowBlockIndexByGroup.set(group, -1);
            return k > 0;
        }

        int neededOverflow = 0;
        int remaining      = totalLocal - primaryCap;
        if (remaining > 0) {
            neededOverflow = (remaining + overflowCap - 1) / overflowCap;
        }

        if (neededOverflow >= k) {
            return false;   // nemá zmysel prebalovať – nič neušetríme
        }

        // 3) reálne striasanie – znovu rozložíme záznamy do primárneho + prvých neededOverflow blokov

        for (int idx : chain) {
            setOverflowNext(idx, -1);
        }

        java.util.Iterator<T> it = records.iterator();

        Block<T> newPrimary = new Block<>(primaryIdx,
                primaryCap,
                recSize,
                recordClass, true);
        while (it.hasNext() && !newPrimary.isFull()) {
            newPrimary.insert(it.next());
        }
        primaryFile.writeBlock(primaryIdx, newPrimary);

        List<Integer> usedOverflow = chain.subList(0, neededOverflow);
        for (int i = 0; i < usedOverflow.size(); i++) {
            int idx = usedOverflow.get(i);
            Block<T> newOv = new Block<>(idx,
                    overflowCap,
                    recSize,
                    recordClass, true);
            while (it.hasNext() && !newOv.isFull()) {
                newOv.insert(it.next());
            }
            overflowFile.writeBlock(idx, newOv);

            int nextIdx = (i + 1 < usedOverflow.size()) ? usedOverflow.get(i + 1) : -1;
            setOverflowNext(idx, nextIdx);
        }

        if (usedOverflow.isEmpty()) {
            firstOverflowBlockIndexByGroup.set(group, -1);
        } else {
            firstOverflowBlockIndexByGroup.set(group, usedOverflow.get(0));
        }

        for (int i2 = neededOverflow; i2 < chain.size(); i2++) {
            int idx = chain.get(i2);
            Block<T> emptyOv = new Block<>(idx,
                    overflowCap,
                    recSize,
                    recordClass, true);
            overflowFile.writeBlock(idx, emptyOv);
            setOverflowNext(idx, -1);
        }

        return true;
    }

    // ===== hustota, split a merge =====

    /**
     * Hustota = počet platných záznamov / počet všetkých slotov (primary + overflow).
     */
    private double computeDensity() {
        long primaryBlocks  = primaryFile.getBlockCount();
        long overflowBlocks = overflowFile.getBlockCount();
        long primarySlots   = primaryBlocks  * primaryFile.getRecordsPerBlock();
        long overflowSlots  = overflowBlocks * overflowFile.getRecordsPerBlock();
        long totalSlots     = primarySlots + overflowSlots;
        if (totalSlots == 0) return 0.0;
        return (double) totalRecords / (double) totalSlots;
    }

    private void trySplitIfNeeded() throws IOException {
        double d = computeDensity();
        if (d <= dMax) {
            return;
        }

        splitGroup(splitPtr);

        splitPtr++;
        int B = baseGroupCount();
        if (splitPtr >= B) {
            splitPtr = 0;
            level++;
        }
    }

    private void tryMergeIfNeeded() throws IOException {
        double d = computeDensity();
        if (d >= dMin) {
            return;
        }

        if (groupCount <= initialGroupCount) {
            return; // nechceme zmenšovať pod M
        }

        int B = baseGroupCount();

        if (splitPtr > 0) {
            int fromGroup = (splitPtr - 1) + B;
            int toGroup   = splitPtr - 1;
            mergeGroups(fromGroup, toGroup);
            splitPtr--;
        } else if (level > 0) {
            level--;
            int newB      = baseGroupCount();
            int toGroup   = newB - 1;
            int fromGroup = toGroup + newB; // posledná skupina
            mergeGroups(fromGroup, toGroup);
            splitPtr = newB - 1;
        }
    }

    /**
     * Rozdelí skupinu groupIndex na groupIndex a (groupIndex + B).
     *
     * Optimalizovaná verzia:
     *  - každý blok (hlavný + všetky overflow) sa pri splite číta práve raz,
     *  - zapisuje sa raz už vo finálnom stave,
     *  - overflow bloky sa recyklujú na tých istých adresách,
     *    prípadné „navyše“ sa vyprázdnia a zaradia do free-listu.
     */
    private void splitGroup(int groupIndex) throws IOException {
        int B        = baseGroupCount();
        int newGroup = groupIndex + B;

        // zabezpečí, že existuje primárny blok pre novú skupinu
        ensureGroupExists(newGroup);

        int primaryIdxOld = primaryBlockIndexByGroup.get(groupIndex);
        int primaryIdxNew = primaryBlockIndexByGroup.get(newGroup);

        // --- 1) nazbierame všetky overflow bloky danej skupiny (iba indexy) ---
        int firstOvOld = firstOverflowBlockIndexByGroup.get(groupIndex);
        List<Integer> chain = new ArrayList<>();
        int current = firstOvOld;
        while (current != -1) {
            chain.add(current);
            current = getOverflowNext(current);
        }

        // --- 2) načítame starý primárny blok + všetky overflow bloky JEDENKRÁT ---
        Block<T> oldPrimaryBlock = primaryFile.readBlock(primaryIdxOld);
        int primaryCap  = oldPrimaryBlock.getCapacity();
        int recSize     = oldPrimaryBlock.getRecordSize();
        int overflowCap = overflowFile.getRecordsPerBlock();

        List<T> allRecords = new ArrayList<>();

        // záznamy z primárneho
        for (int i = 0; i < oldPrimaryBlock.getCapacity(); i++) {
            T r = oldPrimaryBlock.getRecord(i);
            if (r != null) {
                allRecords.add(r);
            }
        }

        // záznamy z overflow blokov
        for (int idx : chain) {
            Block<T> ov = overflowFile.readBlock(idx);
            for (int i = 0; i < ov.getCapacity(); i++) {
                T r = ov.getRecord(i);
                if (r != null) {
                    allRecords.add(r);
                }
            }
        }

        // ak v skupine neboli žiadne záznamy
        if (allRecords.isEmpty()) {
            Block<T> emptyOld = new Block<>(primaryIdxOld,
                    primaryCap,
                    recSize,
                    recordClass, true);
            primaryFile.writeBlock(primaryIdxOld, emptyOld);

            Block<T> emptyNew = new Block<>(primaryIdxNew,
                    primaryCap,
                    recSize,
                    recordClass, true);
            primaryFile.writeBlock(primaryIdxNew, emptyNew);

            for (int idx : chain) {
                Block<T> emptyOv = new Block<>(idx,
                        overflowCap,
                        recSize,
                        recordClass, true);
                overflowFile.writeBlock(idx, emptyOv);
                setOverflowNext(idx, -1);
            }
            firstOverflowBlockIndexByGroup.set(groupIndex, -1);
            firstOverflowBlockIndexByGroup.set(newGroup, -1);
            return;
        }

        // --- 3) rozdelíme záznamy podľa novej hash funkcie na dve skupiny ---
        List<T> forOld = new ArrayList<>();
        List<T> forNew = new ArrayList<>();
        int mod2B = B * 2;

        for (T r : allRecords) {
            String id = r.getId();
            if (id == null) {
                // bezpečný fallback – necháme v pôvodnej skupine
                forOld.add(r);
                continue;
            }
            int h = positiveHash(id);
            int bucket = h % mod2B;
            if (bucket < 0) bucket += mod2B;

            if (bucket == newGroup) {
                forNew.add(r);
            } else {
                forOld.add(r);
            }
        }

        // --- 4) spočítame, koľko overflow blokov potrebujeme pre každú skupinu ---

        int oldOverflowNeeded = 0;
        int newOverflowNeeded = 0;

        int oldRemaining = forOld.size() - primaryCap;
        if (oldRemaining > 0) {
            oldOverflowNeeded = (oldRemaining + overflowCap - 1) / overflowCap;
        }

        int newRemaining = forNew.size() - primaryCap;
        if (newRemaining > 0) {
            newOverflowNeeded = (newRemaining + overflowCap - 1) / overflowCap;
        }

        int k = chain.size();
        int totalNeededOverflow = oldOverflowNeeded + newOverflowNeeded;

        // bezpečnostná poistka – ak by náhodou bolo treba viac overflow blokov, doalokujeme
        if (totalNeededOverflow > k) {
            int extra = totalNeededOverflow - k;
            for (int e = 0; e < extra; e++) {
                int idx = overflowFile.allocateEmptyBlockOrReuse();
                chain.add(idx);
            }
            k = chain.size();
        }

        // --- 5) rozdelíme fyzické overflow bloky medzi starú a novú skupinu ---

        List<Integer> oldOverflowIdx = new ArrayList<>();
        List<Integer> newOverflowIdx = new ArrayList<>();
        int pos = 0;

        for (int i = 0; i < oldOverflowNeeded && pos < k; i++) {
            oldOverflowIdx.add(chain.get(pos++));
        }
        for (int i = 0; i < newOverflowNeeded && pos < k; i++) {
            newOverflowIdx.add(chain.get(pos++));
        }

        // všetkým overflow blokom zatiaľ zrušíme väzby
        for (int idx : chain) {
            setOverflowNext(idx, -1);
        }

        // --- 6) vytvoríme nové primárne bloky v OP a naplníme ich ---

        Block<T> newOldPrimary = new Block<>(primaryIdxOld,
                primaryCap,
                recSize,
                recordClass, true);
        int pOld = 0;
        while (pOld < forOld.size() && !newOldPrimary.isFull()) {
            newOldPrimary.insert(forOld.get(pOld++));
        }
        primaryFile.writeBlock(primaryIdxOld, newOldPrimary);

        Block<T> newNewPrimary = new Block<>(primaryIdxNew,
                primaryCap,
                recSize,
                recordClass, true);
        int pNew = 0;
        while (pNew < forNew.size() && !newNewPrimary.isFull()) {
            newNewPrimary.insert(forNew.get(pNew++));
        }
        primaryFile.writeBlock(primaryIdxNew, newNewPrimary);

        // --- 7) naplníme overflow bloky pre starú skupinu ---

        int prevIdx = -1;
        for (int idx : oldOverflowIdx) {
            Block<T> b = new Block<>(idx,
                    overflowCap,
                    recSize,
                    recordClass, true);
            while (pOld < forOld.size() && !b.isFull()) {
                b.insert(forOld.get(pOld++));
            }
            overflowFile.writeBlock(idx, b);

            if (prevIdx == -1) {
                firstOverflowBlockIndexByGroup.set(groupIndex, idx);
            } else {
                setOverflowNext(prevIdx, idx);
            }
            prevIdx = idx;
        }
        if (oldOverflowIdx.isEmpty()) {
            firstOverflowBlockIndexByGroup.set(groupIndex, -1);
        }

        // --- 8) naplníme overflow bloky pre novú skupinu ---

        prevIdx = -1;
        for (int idx : newOverflowIdx) {
            Block<T> b = new Block<>(idx,
                    overflowCap,
                    recSize,
                    recordClass, true);
            while (pNew < forNew.size() && !b.isFull()) {
                b.insert(forNew.get(pNew++));
            }
            overflowFile.writeBlock(idx, b);

            if (prevIdx == -1) {
                firstOverflowBlockIndexByGroup.set(newGroup, idx);
            } else {
                setOverflowNext(prevIdx, idx);
            }
            prevIdx = idx;
        }
        if (newOverflowIdx.isEmpty()) {
            firstOverflowBlockIndexByGroup.set(newGroup, -1);
        }

        // --- 9) zvyšné overflow bloky (ak sú) vyprázdnime a zaradíme ako voľné ---

        for (int i = pos; i < k; i++) {
            int idx = chain.get(i);
            Block<T> emptyOv = new Block<>(idx,
                    overflowCap,
                    recSize,
                    recordClass, true);
            overflowFile.writeBlock(idx, emptyOv);
            setOverflowNext(idx, -1);
        }
    }

    /**
     * Spojí fromGroup do toGroup. fromGroup MUSÍ byť posledná skupina.
     */
    private void mergeGroups(int fromGroup, int toGroup) throws IOException {
        if (fromGroup < 0 || fromGroup >= groupCount) {
            throw new IllegalArgumentException("fromGroup out of range: " + fromGroup);
        }
        if (toGroup < 0 || toGroup >= groupCount) {
            throw new IllegalArgumentException("toGroup out of range: " + toGroup);
        }
        if (fromGroup == toGroup) return;
        if (fromGroup != groupCount - 1) {
            throw new IllegalStateException("mergeGroups expects fromGroup == last group");
        }

        List<T> toMove = collectRecordsFromGroup(fromGroup);
        clearGroupBlocks(fromGroup);

        for (T r : toMove) {
            insertIntoGroup(toGroup, r);
        }

        primaryBlockIndexByGroup.remove(fromGroup);
        firstOverflowBlockIndexByGroup.remove(fromGroup);

        groupCount--;
    }

    /**
     * Vráti všetky záznamy v skupine (primárny + overflow).
     */
    private List<T> collectRecordsFromGroup(int group) throws IOException {
        List<T> result = new ArrayList<>();
        if (group < 0 || group >= groupCount) {
            return result;
        }

        int primaryIdx = primaryBlockIndexByGroup.get(group);
        Block<T> primaryBlock = primaryFile.readBlock(primaryIdx);

        for (int i = 0; i < primaryBlock.getCapacity(); i++) {
            T r = primaryBlock.getRecord(i);
            if (r != null) {
                result.add(r);
            }
        }

        int current = firstOverflowBlockIndexByGroup.get(group);
        while (current != -1) {
            Block<T> ovBlock = overflowFile.readBlock(current);
            for (int i = 0; i < ovBlock.getCapacity(); i++) {
                T r = ovBlock.getRecord(i);
                if (r != null) {
                    result.add(r);
                }
            }
            current = getOverflowNext(current);
        }

        return result;
    }

    /**
     * Vyprázdni primárny blok aj všetky overflow bloky danej skupiny.
     * (Používa sa pri merge / čistení, nie pri splite.)
     */
    private void clearGroupBlocks(int group) throws IOException {
        if (group < 0 || group >= groupCount) {
            return;
        }

        int primaryIdx   = primaryBlockIndexByGroup.get(group);
        Block<T> primary = primaryFile.readBlock(primaryIdx);
        int primaryCap   = primary.getCapacity();
        int recSize      = primary.getRecordSize();

        Block<T> emptyPrimary = new Block<>(primaryIdx,
                primaryCap,
                recSize,
                recordClass, true);
        primaryFile.writeBlock(primaryIdx, emptyPrimary);

        int overflowCap = overflowFile.getRecordsPerBlock();

        int current = firstOverflowBlockIndexByGroup.get(group);
        while (current != -1) {
            Block<T> emptyOv = new Block<>(current,
                    overflowCap,
                    recSize,
                    recordClass, true);
            overflowFile.writeBlock(current, emptyOv);

            int next = getOverflowNext(current);
            setOverflowNext(current, -1);
            current = next;
        }

        firstOverflowBlockIndexByGroup.set(group, -1);
    }

    // ===== META – uloženie / načítanie =====

    private void saveMeta() {
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(metaPath)))) {

            dos.writeInt(META_MAGIC);
            dos.writeInt(META_VERSION);

            dos.writeInt(initialGroupCount);
            dos.writeDouble(dMax);
            dos.writeDouble(dMin);

            dos.writeInt(level);
            dos.writeInt(splitPtr);
            dos.writeInt(groupCount);
            dos.writeLong(totalRecords);

            dos.writeInt(primaryBlockIndexByGroup.size());
            for (int idx : primaryBlockIndexByGroup) {
                dos.writeInt(idx);
            }

            dos.writeInt(firstOverflowBlockIndexByGroup.size());
            for (int idx : firstOverflowBlockIndexByGroup) {
                dos.writeInt(idx);
            }

            dos.writeInt(overflowNext.size());
            for (int v : overflowNext) {
                dos.writeInt(v);
            }

        } catch (IOException e) {
            throw new IllegalStateException("Error saving LinHash meta", e);
        }
    }

    private void loadMeta() {
        File f = new File(metaPath);
        if (!f.exists()) {
            throw new IllegalStateException("Meta file not found: " + metaPath);
        }

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(f)))) {

            int magic = dis.readInt();
            if (magic != META_MAGIC) {
                throw new IllegalStateException("Invalid LinHash meta magic");
            }
            int version = dis.readInt();
            if (version != META_VERSION) {
                throw new IllegalStateException("Unsupported LinHash meta version: " + version);
            }

            int metaM = dis.readInt();
            if (metaM != initialGroupCount) {
                throw new IllegalStateException("initialGroupCount mismatch in meta");
            }
            double metaDmax = dis.readDouble();
            double metaDmin = dis.readDouble();
            // podľa potreby sa dá skontrolovať zhoda s dMax/dMin

            this.level        = dis.readInt();
            this.splitPtr     = dis.readInt();
            this.groupCount   = dis.readInt();
            this.totalRecords = dis.readLong();

            primaryBlockIndexByGroup.clear();
            int pSize = dis.readInt();
            for (int i = 0; i < pSize; i++) {
                primaryBlockIndexByGroup.add(dis.readInt());
            }

            firstOverflowBlockIndexByGroup.clear();
            int fSize = dis.readInt();
            for (int i = 0; i < fSize; i++) {
                firstOverflowBlockIndexByGroup.add(dis.readInt());
            }

            overflowNext.clear();
            int ovSize = dis.readInt();
            for (int i = 0; i < ovSize; i++) {
                overflowNext.add(dis.readInt());
            }

        } catch (IOException e) {
            throw new IllegalStateException("Error loading LinHash meta", e);
        }
    }

    // ===== debug / close =====

    /**
     * Sekvenčný výpis štruktúry – použiteľné pre GUI debug. 
     */
    public String dumpStructure() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("LinHashFile dump:\n");
        sb.append("M=").append(initialGroupCount)
          .append(", level=").append(level)
          .append(", S=").append(splitPtr)
          .append(", groupCount=").append(groupCount)
          .append(", totalRecords=").append(totalRecords).append("\n");
        sb.append("primaryBlocks=").append(primaryFile.getBlockCount())
          .append(", overflowBlocks=").append(overflowFile.getBlockCount()).append("\n\n");

        // množina už vypísaných primárnych blokov – (ak by si chcel odstrániť duplicity, dá sa použiť)
        Set<Integer> printedPrimary = new HashSet<>();

        for (int g = 0; g < groupCount; g++) {
            int pIdx = primaryBlockIndexByGroup.get(g);

            sb.append("Group ").append(g).append(":\n");
            Block<T> pBlock = primaryFile.readBlock(pIdx);
            sb.append("  primaryIndex=").append(pIdx)
              .append(", validCount=").append(pBlock.getValidCount())
              .append(", capacity=").append(pBlock.getCapacity()).append("\n");
            for (int i = 0; i < pBlock.getCapacity(); i++) {
                T r = pBlock.getRecord(i);
                sb.append("    [P ").append(i).append("] ")
                  .append(r != null ? r.toString() : "<empty>").append("\n");
            }

            int firstOv = firstOverflowBlockIndexByGroup.get(g);
            sb.append("  firstOverflowIndex=").append(firstOv).append("\n");
            int current = firstOv;
            int chainPos = 0;
            while (current != -1) {
                Block<T> ov = overflowFile.readBlock(current);
                sb.append("    Overflow #").append(chainPos)
                  .append(" blockIndex=").append(current)
                  .append(", validCount=").append(ov.getValidCount()).append("\n");
                for (int i = 0; i < ov.getCapacity(); i++) {
                    T r = ov.getRecord(i);
                    sb.append("      [O ").append(i).append("] ")
                      .append(r != null ? r.toString() : "<empty>").append("\n");
                }
                current = getOverflowNext(current);
                chainPos++;
            }
            sb.append("\n");
        }

        return sb.toString();
    }

    @Override
    public void close() throws IOException {
        saveMeta();
        primaryFile.close();
        overflowFile.close();
    }
}
