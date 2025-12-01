package aus2_sem2.controller;

import aus2_sem2.model.PatientRecord;
import aus2_sem2.storage.HeapFile;
import aus2_sem2.storage.LinHashFile;

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
 * Controller – sprostredkuje prácu medzi GUI a dátovými súbormi.
 * - Pracuje s HeapFile<PatientRecord> (fyzické uloženie záznamov).
 * - Nad rovnakým súborom buduje index LinHashFile<PatientRecord>.
 *
 * Všetky vkladania a mazania v „produkčnej“ časti aplikácie idú cez LinHashFile,
 * aby bol index vždy konzistentný s obsahom HeapFile.
 */
public class PatientFileController {

    private final HeapFile<PatientRecord> heapFile;            // model – fyzický súbor
    private final LinHashFile<PatientRecord> linHashFile;      // lineárne hešovanie nad tým istým súborom
    private final Random random;

    // auto-increment ID pacienta (unikátne v rámci systému)
    private static final String ID_META_FILE = "heapfile_patients_id.meta";

    // meta súbor pre LinHash index
    private static final String LINHASH_META_FILE = "patients_linhash.meta";

    /**
     * Počítadlo ďalšieho ID pacienta.
     * Používame typ int ako „nezáporný čítač“, pre účely zadania plne postačuje.
     */
    private int nextPatientId;

    public PatientFileController(HeapFile<PatientRecord> heapFile) {
        this.heapFile = heapFile;
        this.random = new Random();

        // načítame počítadlo ID z meta súboru
        this.nextPatientId = loadNextPatientId();

        // vytvoríme index LinHashFile nad tým istým HeapFile
        this.linHashFile = new LinHashFile<>(heapFile, LINHASH_META_FILE);

        // ak je súbor pri štarte úplne prázdny, resetujeme čítač ID na 1
        if (this.heapFile.getTotalValidRecords() == 0) {
            this.nextPatientId = 1;
            saveNextPatientId();
        }
    }

    // --- info pre GUI ---

    /** Vráti textový dump dátového súboru (HeapFile). */
    public String getDump() {
        return heapFile.dumpDebugInfo();
    }

    /** Vráti textový dump lineárneho hešovacieho indexu (LinHashFile). */
    public String getLinHashDump() {
        return linHashFile.debugDump();
    }

    /** Vráti počet platných záznamov v HeapFile. */
    public int getTotalRecords() {
        return heapFile.getTotalValidRecords();
    }

    // --- mazanie (náhodné alebo podľa ID) ---

    /**
     * Zmaže náhodné záznamy zo súboru.
     * Mazanie prebieha CEZ LinHashFile.deleteById(), aby sa udržal aj index.
     */
    public int deleteRandomRecords(int count) {
        List<Long> addresses = heapFile.getAllAddresses();
        if (addresses.isEmpty() || count <= 0) return 0;
        if (count > addresses.size()) count = addresses.size();

        Collections.shuffle(addresses, random);

        int removed = 0;
        for (int i = 0; i < count; i++) {
            long addr = addresses.get(i);
            PatientRecord rec = heapFile.get(addr);
            if (rec == null) {
                continue;
            }
            String id = rec.getId();
            if (id == null) {
                continue;
            }
            if (linHashFile.deleteById(id)) {
                removed++;
            }
        }
        return removed;
    }

    /**
     * Zmaže prvý záznam so zadaným ID cez LinHash index.
     */
    public boolean deleteById(String id) {
        if (id == null || id.isEmpty()) return false;
        return linHashFile.deleteById(id);
    }

    // --- funkčný test (10 insert, 4 delete) ---

    /**
     * Otestuje základnú funkcionalitu vkladania/mazania.
     * - Vkladanie ide cez insertRecordUnique() (auto-ID + LinHashFile.insert()).
     * - Mazanie prebieha cez LinHashFile.deleteById().
     */
    public void runFunctionalTest() {
        List<String> createdIds = new ArrayList<>();

        // vloží 10 testovacích záznamov s auto-ID
        for (int i = 0; i < 10; i++) {
            String meno = "TestM" + i;
            String priez = "TestP" + i;
            String date = String.format("%02d:%02d:%04d",
                    (i % 28) + 1, (i % 12) + 1, 2000 + i);

            PatientRecord recWithoutId = new PatientRecord(meno, priez, date, "");
            String newId = previewNextIdString(); // pozri sa, aké ID bude pridelené
            insertRecordUnique(recWithoutId);     // reálne vloženie s auto-ID
            createdIds.add(newId);
        }

        // zmaže 4 z nich cez ich ID
        int[] toDeleteIdx = {1, 3, 5, 7};
        for (int idx : toDeleteIdx) {
            if (idx < createdIds.size()) {
                String id = createdIds.get(idx);
                linHashFile.deleteById(id);
            }
        }
    }

    // --- LinHash-only pomocné metódy pre druhú záložku GUI ---

    /**
     * Vyhľadá záznam podľa ID cez LinHash index.
     * Používa sa v záložke „LinHashFile“ v GUI.
     */
    public PatientRecord linFindById(String id) {
        if (id == null || id.isEmpty()) return null;
        return linHashFile.findById(id);
    }

    /**
     * Zmaže záznam podľa ID cez LinHash index.
     * Používa sa v záložke „LinHashFile“ v GUI.
     */
    public boolean linDeleteById(String id) {
        if (id == null || id.isEmpty()) return false;
        return linHashFile.deleteById(id);
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
        long addr = linHashFile.insert(withId); // insert ide cez index
        saveNextPatientId();                    // uložíme aktuálnu hodnotu počítadla
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
            linHashFile.insert(withId);
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
     * „Nehýbe“ počítadlom – len vráti ID, ktoré by bolo vygenerované pri najbližšom inserte.
     * Používa sa v runFunctionalTest na zapamätanie si ID ešte pred vložením.
     */
    private String previewNextIdString() {
        int tmp = nextPatientId;
        if (tmp <= 0) {
            tmp = 1;
        }
        return String.format("%010d", tmp);
    }

    /**
     * Vygeneruje nový ID reťazec v tvare "0000000001", "0000000002", ...
     * a inkrementuje počítadlo.
     *
     * Obsahuje logiku:
     *  - ak je HeapFile v danom momente prázdny (0 záznamov), resetuje počítadlo na 1,
     *    aby po kompletom vymazaní súboru išlo ID znova od 1.
     */
    private String generateNextIdString() {
        // ak je dátový súbor prázdny, začíname s ID = 1
        if (heapFile.getTotalValidRecords() == 0) {
            nextPatientId = 1;
        }

        if (nextPatientId <= 0) {
            // ochrana pre prípad poškodenia meta súboru alebo overflow
            nextPatientId = 1;
        }

        String result = String.format("%010d", nextPatientId);
        nextPatientId++;

        // jednoduchá ochrana pred pretečením – po MIN_VALUE reset na 1,
        // v reálnej úlohe sa na tento limit prakticky nedostaneme
        if (nextPatientId == Integer.MIN_VALUE) {
            nextPatientId = 1;
        }

        return result;
    }

    /** Volateľné pri korektnom ukončení aplikácie. */
    public void close() {
        saveNextPatientId();
        if (linHashFile != null) {
            // LinHashFile pri close() zavrie aj HeapFile
            linHashFile.close();
        } else {
            heapFile.close();
        }
    }
}
