package aus2_sem2.controller;

import aus2_sem2.model.PatientRecord;
import aus2_sem2.model.PCRTestRecord;
import aus2_sem2.storage.HeapFile;
import aus2_sem2.storage.LinHashFile;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * PcrDatabaseService – servisná vrstva pre semestrálnu prácu S2.
 *
 * Implementuje 8 operácií zadania s dvomi indexmi:
 *   - personIndex: LinHashFile<PatientRecord>, kľúč = patientId (getId()).
 *   - testIndex:   LinHashFile<PCRTestRecord>, kľúč = testCode (getId()).
 *
 * Navyše:
 *   - automatické generovanie ID pacienta (String, 10 číslic);
 *   - automatické generovanie kódu PCR testu (int);
 *   - hodnoty čítačov sa uchovávajú v meta súboroch.
 *
 * Rozšírenie:
 *   - generovanie náhodných pacientov;
 *   - generovanie náhodných testov pre existujúcich pacientov.
 */
public class PcrDatabaseService {

    // ===== DTO pre operácie =====

    public static class PersonWithTests {
        private final PatientRecord person;
        private final List<PCRTestRecord> tests;

        public PersonWithTests(PatientRecord person, List<PCRTestRecord> tests) {
            this.person = person;
            this.tests = tests;
        }

        public PatientRecord getPerson() {
            return person;
        }

        public List<PCRTestRecord> getTests() {
            return tests;
        }
    }

    public static class TestWithPerson {
        private final PCRTestRecord test;
        private final PatientRecord person;

        public TestWithPerson(PCRTestRecord test, PatientRecord person) {
            this.test = test;
            this.person = person;
        }

        public PCRTestRecord getTest() {
            return test;
        }

        public PatientRecord getPerson() {
            return person;
        }
    }

    // ===== indexy =====

    private final LinHashFile<PatientRecord> personIndex;
    private final LinHashFile<PCRTestRecord> testIndex;

    // ===== cesty k meta-súborom pre čítače =====

    private final String patientIdMetaPath;
    private final String testCodeMetaPath;

    // ===== čítače =====

    private int nextPatientId; // vždy > 0
    private int nextTestCode;  // vždy > 0

    // ===== generátor náhodných čísel na generovanie dát =====

    private final Random random = new Random();

    /**
     * Konštruktor služby.
     *
     * @param basePathPersons základná cesta pre index pacientov (napr. "patients")
     * @param basePathTests   základná cesta pre index testov (napr. "tests")
     */
    public PcrDatabaseService(String basePathPersons, String basePathTests, int blockSize) {
        try {
            this.personIndex = new LinHashFile<>(
                    basePathPersons,
                    blockSize,
                    PatientRecord.class,
                    4,
                    0.75,
                    0.40
            );

            this.testIndex = new LinHashFile<>(
                    basePathTests,
                    blockSize,
                    PCRTestRecord.class,
                    4,
                    0.75,
                    0.40
            );
        } catch (IOException e) {
            throw new IllegalStateException("Ошибка инициализации PcrDatabaseService: " + e.getMessage(), e);
        }

        this.patientIdMetaPath = basePathPersons + "_id_counter.dat";
        this.testCodeMetaPath = basePathTests + "_code_counter.dat";

        this.nextPatientId = loadCounter(patientIdMetaPath);
        this.nextTestCode = loadCounter(testCodeMetaPath);
    }

    // =========================================================
    // 1. Vloženie výsledku PCR testu (ručná verzia, podľa hotového objektu)
    // =========================================================

    /**
     * 1. Vloženie výsledku PCR testu do systému (ručná verzia – ak už existuje objekt s kódom).
     *
     * Systém:
     *  - kontroluje existenciu pacienta,
     *  - kontroluje jedinečnosť kódu testu,
     *  - kontroluje maximum 6 testov na jednu osobu.
     *
     * @return true, ak bolo vloženie úspešné.
     */
    public synchronized boolean insertTest(PCRTestRecord test) {
        if (test == null) {
            throw new IllegalArgumentException("Test nesmie byť null.");
        }
        String patientId = test.getPatientId();
        if (patientId == null || patientId.isEmpty()) {
            throw new IllegalArgumentException("Test musí mať vyplnené patientId.");
        }

        try {
            // 1) pacient musí existovať
            PatientRecord patient = personIndex.findById(patientId);
            if (patient == null) {
                return false;
            }

            // 2) kód testu musí byť jedinečný
            String testKey = test.getId(); // getId() = String.valueOf(testCode)
            if (testIndex.findById(testKey) != null) {
                return false;
            }

            // 3) najviac 6 testov na jedného pacienta
            int countForPatient = countTestsForPatient(patientId);
            if (countForPatient >= 6) {
                return false;
            }

            // 4) vloženie
            testIndex.insert(test);
            return true;

        } catch (IOException e) {
            throw new IllegalStateException("Chyba pri insertTest(): " + e.getMessage(), e);
        }
    }

    // =========================================================
    // 1. Vloženie testu s automatickým ID
    // =========================================================

    /**
     * Automatická verzia operácie 1:
     *  - kód testu sa generuje automaticky (int),
     *  - pacient sa zadáva iba podľa svojho ID,
     *  - čítač testov sa uloží na disk.
     *
     * @return vložený PCRTestRecord s prideleným kódom testu,
     *         alebo null, ak vloženie nie je možné
     *         (neexistujúci pacient / už existuje 6 testov).
     */
    public synchronized PCRTestRecord insertTestAuto(
            String patientId,
            String dateTime,
            boolean result,
            double value,
            String note
    ) {
        if (patientId == null || patientId.isEmpty()) {
            throw new IllegalArgumentException("patientId nesmie byť prázdne.");
        }

        try {
            // overíme pacienta a limit 6 testov ešte pred generovaním kódu
            PatientRecord patient = personIndex.findById(patientId);
            if (patient == null) {
                return null;
            }
            int countForPatient = countTestsForPatient(patientId);
            if (countForPatient >= 6) {
                return null;
            }

            // v tomto bode by malo byť vloženie možné – môžeme rezervovať kód
            int code = (nextTestCode <= 0) ? 1 : nextTestCode;

            PCRTestRecord test = new PCRTestRecord(
                    dateTime,
                    patientId,
                    code,
                    result,
                    value,
                    note
            );

            boolean ok = insertTest(test); // znovu použije kontroly, ale tie už by mali prejsť
            if (!ok) {
                // teoreticky by sa to nemalo stať, ale pre istotu:
                return null;
            }

            // úspešné vloženie → posunieme čítač a uložíme na disk
            nextTestCode = code + 1;
            saveCounter(testCodeMetaPath, nextTestCode);

            return test;

        } catch (IOException e) {
            throw new IllegalStateException("Chyba pri insertTestAuto(): " + e.getMessage(), e);
        }
    }

    // =========================================================
    // 2. Nájsť osobu + jej testy
    // =========================================================

    public PersonWithTests findPersonWithTests(String patientId) {
        if (patientId == null || patientId.isEmpty()) {
            return null;
        }

        try {
            PatientRecord person = personIndex.findById(patientId);
            if (person == null) {
                return null;
            }
            List<PCRTestRecord> tests = getTestsForPatient(patientId);
            return new PersonWithTests(person, tests);
        } catch (IOException e) {
            throw new IllegalStateException("Chyba pri findPersonWithTests(): " + e.getMessage(), e);
        }
    }

    // =========================================================
    // 3. Nájsť test + osobu
    // =========================================================

    public TestWithPerson findTestWithPerson(int testCode) {
        String testKey = String.valueOf(testCode);
        try {
            PCRTestRecord test = testIndex.findById(testKey);
            if (test == null) {
                return null;
            }
            String patientId = test.getPatientId();
            PatientRecord person = null;
            if (patientId != null && !patientId.isEmpty()) {
                person = personIndex.findById(patientId);
            }
            return new TestWithPerson(test, person);
        } catch (IOException e) {
            throw new IllegalStateException("Chyba pri findTestWithPerson(): " + e.getMessage(), e);
        }
    }

    // =========================================================
    // 4. Vloženie osoby (ručná verzia – ak už ID existuje)
    // =========================================================

    public synchronized boolean insertPerson(PatientRecord person) {
        if (person == null) {
            throw new IllegalArgumentException("Person nesmie byť null.");
        }
        String id = person.getId();
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Person musí mať vyplnené unikátne číslo pacienta (ID).");
        }

        try {
            if (personIndex.findById(id) != null) {
                return false;
            }
            personIndex.insert(person);
            return true;
        } catch (IOException e) {
            throw new IllegalStateException("Chyba pri insertPerson(): " + e.getMessage(), e);
        }
    }

    // =========================================================
    // 4. Vloženie osoby s auto-ID
    // =========================================================

    /**
     * Automatická verzia operácie 4:
     *  - ID pacienta sa generuje automaticky ako 10-miestny reťazec ("0000000001", ...).
     *
     * @return vložený PatientRecord s prideleným ID,
     *         alebo null, ak sa nepodarilo vložiť (extrémne zriedkavé).
     */
    public synchronized PatientRecord insertPersonAuto(
            String meno,
            String priezvisko,
            String birthDate
    ) {
        if (meno == null || meno.isEmpty()
                || priezvisko == null || priezvisko.isEmpty()
                || birthDate == null || birthDate.isEmpty()) {
            throw new IllegalArgumentException("Meno, priezvisko a dátum narodenia musia byť vyplnené.");
        }

        int idInt = (nextPatientId <= 0) ? 1 : nextPatientId;
        String idStr = formatPatientId(idInt);

        PatientRecord person = new PatientRecord(meno, priezvisko, birthDate, idStr);
        boolean ok = insertPerson(person);
        if (!ok) {
            // teoreticky by sa to nemalo stať (ID je vždy nové), ale aby sme nič nerozbili:
            return null;
        }

        nextPatientId = idInt + 1;
        saveCounter(patientIdMetaPath, nextPatientId);

        return person;
    }

    private String formatPatientId(int idInt) {
        // 10 číslic, úvodné nuly: 0000000001, 0000000002, ...
        return String.format("%010d", idInt);
    }

    // =========================================================
    // 5. Vymazanie testu
    // =========================================================

    public synchronized boolean deleteTestByCode(int testCode) {
        String testKey = String.valueOf(testCode);
        try {
            return testIndex.deleteById(testKey);
        } catch (IOException e) {
            throw new IllegalStateException("Chyba pri deleteTestByCode(): " + e.getMessage(), e);
        }
    }

    // =========================================================
    // 6. Vymazanie osoby + jej testov
    // =========================================================

    public synchronized boolean deletePersonAndTests(String patientId) {
        if (patientId == null || patientId.isEmpty()) {
            return false;
        }

        try {
            PatientRecord person = personIndex.findById(patientId);
            if (person == null) {
                return false;
            }

            List<PCRTestRecord> tests = getTestsForPatient(patientId);
            for (PCRTestRecord t : tests) {
                String testKey = t.getId();
                if (testKey != null) {
                    testIndex.deleteById(testKey);
                }
            }

            personIndex.deleteById(patientId);

            return true;
        } catch (IOException e) {
            throw new IllegalStateException("Chyba pri deletePersonAndTests(): " + e.getMessage(), e);
        }
    }

    // =========================================================
    // 7. Editácia osoby
    // =========================================================

    public synchronized boolean updatePerson(PatientRecord updated) {
        if (updated == null) {
            return false;
        }
        String id = updated.getId();
        if (id == null || id.isEmpty()) {
            return false;
        }

        try {
            return personIndex.editById(updated);
        } catch (IOException e) {
            throw new IllegalStateException("Chyba pri updatePerson(): " + e.getMessage(), e);
        }
    }

    // =========================================================
    // 8. Editácia testu
    // =========================================================

    public synchronized boolean updateTest(PCRTestRecord updated) {
        if (updated == null) {
            return false;
        }
        String key = updated.getId();
        if (key == null || key.isEmpty()) {
            return false;
        }

        try {
            return testIndex.editById(updated);
        } catch (IOException e) {
            throw new IllegalStateException("Chyba pri updateTest(): " + e.getMessage(), e);
        }
    }

    // =========================================================
    // Doplnkové metódy pre GUI
    // =========================================================

    public String dumpPersonsIndex() {
        try {
            return personIndex.dumpStructure();
        } catch (IOException e) {
            return "Chyba pri dumpPersonsIndex(): " + e.getMessage();
        }
    }

    public String dumpTestsIndex() {
        try {
            return testIndex.dumpStructure();
        } catch (IOException e) {
            return "Chyba pri dumpTestsIndex(): " + e.getMessage();
        }
    }

    public long getPersonCount() {
        return personIndex.getTotalRecords();
    }

    public long getTestCount() {
        return testIndex.getTotalRecords();
    }

    public void close() {
        try {
            personIndex.close();
        } catch (IOException ignored) {}
        try {
            testIndex.close();
        } catch (IOException ignored) {}
    }

    // =========================================================
    // Generovanie náhodných pacientov a testov
    // =========================================================

    /**
     * Vygeneruje zadaný počet pacientov s náhodnými údajmi.
     *
     * @param count počet pacientov, o ktorý sa pokúsime
     * @return skutočný počet vytvorených záznamov
     */
    public synchronized int generateRandomPatients(int count) {
        if (count <= 0) {
            return 0;
        }
        int inserted = 0;
        for (int i = 0; i < count; i++) {
            String meno = randomFirstName();
            String priezvisko = randomLastName();
            String birth = randomBirthDate();

            PatientRecord p = insertPersonAuto(meno, priezvisko, birth);
            if (p != null) {
                inserted++;
            } else {
                // veľmi nepravdepodobné, ale ak by náhodou došlo ku kolízii ID alebo inej chybe – skončíme
                break;
            }
        }
        return inserted;
    }

    /**
     * Vygeneruje zadaný počet testov pre už existujúcich pacientov.
     * Rešpektuje obmedzenie maximum 6 testov na pacienta.
     *
     * @param count počet testov, o ktorý sa pokúsime
     * @return skutočný počet vytvorených testov
     */
    public synchronized int generateRandomTestsForExistingPatients(int count) {
        if (count <= 0) {
            return 0;
        }

        try {
            List<PatientRecord> patients = getAllPatientsInternal();
            if (patients.isEmpty()) {
                return 0;
            }

            int n = patients.size();
            int[] testCounts = new int[n];
            for (int i = 0; i < n; i++) {
                testCounts[i] = countTestsForPatient(patients.get(i).getId());
            }

            int inserted = 0;
            int maxAttempts = n * 6 * 2;
            int attempts = 0;

            while (inserted < count && attempts < maxAttempts) {
                attempts++;

                int idx = random.nextInt(n);
                if (testCounts[idx] >= 6) {
                    continue;
                }

                PatientRecord p = patients.get(idx);
                String patientId = p.getId();

                String dt = randomTestDateTime();
                boolean result = random.nextBoolean();
                double value = randomTestValue();
                String note = randomTestNote();

                PCRTestRecord t = insertTestAuto(patientId, dt, result, value, note);
                if (t != null) {
                    testCounts[idx]++;
                    inserted++;
                }
            }

            return inserted;

        } catch (IOException e) {
            throw new IllegalStateException("Chyba pri generateRandomTestsForExistingPatients(): " + e.getMessage(), e);
        }
    }

    // ===== Interné random helpery =====

    private String randomFirstName() {
        String[] names = {
                "Adam", "Peter", "Martin", "Ján", "Lukáš",
                "Lucia", "Jana", "Eva", "Zuzana", "Maria"
        };
        return names[random.nextInt(names.length)];
    }

    private String randomLastName() {
        String[] surnames = {
                "Novák", "Kováč", "Horváth", "Varga", "Tóth",
                "Ferenc", "Kollár", "Baláž", "Král", "Bielik"
        };
        return surnames[random.nextInt(surnames.length)];
    }

    /**
     * Dátum narodenia vo formáte DD:MM:RRRR, roky približne 1950–2010.
     */
    private String randomBirthDate() {
        int year = 1950 + random.nextInt(61);   // 1950–2010
        int month = 1 + random.nextInt(12);     // 1–12
        int day = 1 + random.nextInt(28);       // 1–28, aby sme nemuseli riešiť rôzne dĺžky mesiacov
        return String.format("%02d:%02d:%04d", day, month, year);
    }

    /**
     * Dátum/čas testu vo formáte "DD:MM:RRRR HH:MM", roky približne 2020–2025.
     */
    private String randomTestDateTime() {
        int year = 2020 + random.nextInt(6);    // 2020–2025
        int month = 1 + random.nextInt(12);
        int day = 1 + random.nextInt(28);
        int hour = random.nextInt(24);
        int minute = random.nextInt(60);
        return String.format("%02d:%02d:%04d %02d:%02d", day, month, year, hour, minute);
    }

    private double randomTestValue() {
        // Napríklad 5.000 – 40.000 s 3 desatinnými miestami
        double base = 5.0 + random.nextDouble() * 35.0;
        return Math.round(base * 1000.0) / 1000.0;
    }

    private String randomTestNote() {
        String[] notes = {
                "auto-gen", "kontrola", "screening", "PCR", "urgent",
                "follow-up", "routine", "opakovaný", "interné", "ext."
        };
        String n = notes[random.nextInt(notes.length)];
        // pre istotu obmedzíme dĺžku, ak máš limit 11 znakov
        if (n.length() > 11) {
            return n.substring(0, 11);
        }
        return n;
    }

    // =========================================================
    // Interné helpery (testy, pacienti, čítače)
    // =========================================================

    private int loadCounter(String path) {
        File f = new File(path);
        if (!f.exists()) {
            return 1;
        }
        try (DataInputStream dis = new DataInputStream(new FileInputStream(f))) {
            int value = dis.readInt();
            return (value <= 0) ? 1 : value;
        } catch (IOException e) {
            // pri chybe začneme od 1
            return 1;
        }
    }

    private void saveCounter(String path, int value) {
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(path))) {
            dos.writeInt(value);
        } catch (IOException e) {
            // meta súbor nie je kritický – v najhoršom prípade pri ďalšom spustení začneme od 1
        }
    }

    private int countTestsForPatient(String patientId) throws IOException {
        int count = 0;
        List<PCRTestRecord> all = getAllTestsInternal();
        for (PCRTestRecord t : all) {
            if (patientId.equals(t.getPatientId())) {
                count++;
            }
        }
        return count;
    }

    private List<PCRTestRecord> getTestsForPatient(String patientId) throws IOException {
        List<PCRTestRecord> result = new ArrayList<>();
        List<PCRTestRecord> all = getAllTestsInternal();
        for (PCRTestRecord t : all) {
            if (patientId.equals(t.getPatientId())) {
                result.add(t);
            }
        }
        return result;
    }

    private List<PCRTestRecord> getAllTestsInternal() throws IOException {
        List<PCRTestRecord> out = new ArrayList<>();
        collectTestsFromHeap(testIndex.getPrimaryFile(), out);
        collectTestsFromHeap(testIndex.getOverflowFile(), out);
        return out;
    }

    private void collectTestsFromHeap(HeapFile<PCRTestRecord> heap, List<PCRTestRecord> out)
            throws IOException {
        if (heap == null) return;
        List<Long> addrs = heap.getAllAddresses();
        for (long addr : addrs) {
            PCRTestRecord r = heap.get(addr);
            if (r != null) {
                out.add(r);
            }
        }
    }

    private List<PatientRecord> getAllPatientsInternal() throws IOException {
        List<PatientRecord> out = new ArrayList<>();
        collectPatientsFromHeap(personIndex.getPrimaryFile(), out);
        collectPatientsFromHeap(personIndex.getOverflowFile(), out);
        return out;
    }

    private void collectPatientsFromHeap(HeapFile<PatientRecord> heap, List<PatientRecord> out)
            throws IOException {
        if (heap == null) return;
        List<Long> addrs = heap.getAllAddresses();
        for (long addr : addrs) {
            PatientRecord r = heap.get(addr);
            if (r != null) {
                out.add(r);
            }
        }
    }
}
