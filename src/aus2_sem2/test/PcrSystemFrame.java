package aus2_sem2.test;

import aus2_sem2.controller.PcrDatabaseService;
import aus2_sem2.model.PCRTestRecord;
import aus2_sem2.model.PatientRecord;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.List;

/**
 * PcrSystemFrame – hlavné GUI okno pre semestrálnu prácu S2.
 *
 *  - Využíva PcrDatabaseService (jadro) – žiadny priamy prístup k HeapFile/LinHashFile z GUI.
 *  - Prvá záložka: 8 funkčných operácií podľa zadania.
 *  - Druhá záložka: dump indexov (pre zobrazenie blokov, overflow a pod.).
 *  - Tretia záložka: generátor pacientov a testov.
 */
public class PcrSystemFrame extends JFrame {

    private PcrDatabaseService service;

    // --- Komponenty – sekcia "Vloženie osoby" ---
    private JTextField personIdField;
    private JTextField personNameField;
    private JTextField personSurnameField;
    private JTextField personBirthField;
    private JButton insertPersonButton;

    // --- Komponenty – sekcia "Vloženie testu" ---
    private JTextField testCodeField;
    private JTextField testPatientIdField;
    private JTextField testDateTimeField;
    private JCheckBox testResultCheck;
    private JTextField testValueField;
    private JTextField testNoteField;
    private JButton insertTestButton;

    // --- Komponenty – sekcia "Vyhľadanie / editácia osoby" ---
    private JTextField searchPersonIdField;
    private JButton searchPersonButton;

    private JTextField editPersonIdField;
    private JTextField editPersonNameField;
    private JTextField editPersonSurnameField;
    private JTextField editPersonBirthField;
    private JButton updatePersonButton;

    private JTextArea personTestsArea;
    private JLabel personTestsCountLabel;

    // --- Komponenty – sekcia "Vyhľadanie / editácia testu" ---
    private JTextField searchTestCodeField;
    private JButton searchTestButton;

    private JTextField editTestCodeField;
    private JTextField editTestPatientIdField;
    private JTextField editTestDateTimeField;
    private JCheckBox editTestResultCheck;
    private JTextField editTestValueField;
    private JTextField editTestNoteField;
    private JButton updateTestButton;
    private JButton deleteTestButton;

    // --- Компоненты – секция "Vymazanie osoby + testov" ---
    private JTextField deletePersonIdField;
    private JButton deletePersonButton;

    // --- Dump záložka ---
    private JTextArea dumpTextArea;
    private JButton dumpPersonsButton;
    private JButton dumpTestsButton;

    // --- Generátor záložka ---
    private JTextField genPersonCountField;
    private JButton genPersonsButton;
    private JTextField genTestCountField;
    private JButton genTestsButton;
    
    private JTextField dbPersonsField;
    private JTextField dbTestsField;
    private JTextField dbBlockSizeField;
    private JButton dbOpenButton;

    public PcrSystemFrame() {
        super("WHO PCR Evidence System – S2");

        // jadro
        this.service = new PcrDatabaseService("patients", "tests", 256);

        initUi();
        registerListeners();
    }

    // =========================================================
    // Inicializácia UI
    // =========================================================

    private void initUi() {
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setLayout(new BorderLayout());
        add(createDbPanel(), BorderLayout.NORTH);
        dbOpenButton.addActionListener(e -> handleOpenDb());
        JTabbedPane tabbedPane = new JTabbedPane();

        JPanel operationsPanel = createOperationsPanel();
        JPanel dumpPanel = createDumpPanel();
        JPanel generatorPanel = createGeneratorPanel();

        tabbedPane.addTab("Operácie", operationsPanel);
        tabbedPane.addTab("Dump", dumpPanel);
        tabbedPane.addTab("Generátor", generatorPanel);

        add(tabbedPane, BorderLayout.CENTER);

        pack();
        setLocationRelativeTo(null);
        setMinimumSize(new Dimension(1100, 700));
    }

    private JPanel createOperationsPanel() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));

        panel.add(createInsertPersonPanel());
        panel.add(createInsertTestPanel());
        panel.add(createPersonSearchPanel());
        panel.add(createTestSearchPanel());
        panel.add(createDeletePersonPanel());

        return panel;
    }
    private JPanel createDbPanel() {
    JPanel p = new JPanel(new GridBagLayout());
    p.setBorder(new TitledBorder("DBS konfigurácia"));

    GridBagConstraints gbc = baseGbc();

    dbPersonsField = new JTextField("patients", 12);
    dbTestsField = new JTextField("tests", 12);
    dbBlockSizeField = new JTextField("256", 6);
    dbOpenButton = new JButton("Otvoriť / Vytvoriť DBS");

    gbc.gridx = 0; gbc.gridy = 0;
    p.add(new JLabel("Persons base:"), gbc);
    gbc.gridx = 1;
    p.add(dbPersonsField, gbc);

    gbc.gridx = 2;
    p.add(new JLabel("Tests base:"), gbc);
    gbc.gridx = 3;
    p.add(dbTestsField, gbc);

    gbc.gridx = 4;
    p.add(new JLabel("Block size:"), gbc);
    gbc.gridx = 5;
    p.add(dbBlockSizeField, gbc);

    gbc.gridx = 6;
    p.add(dbOpenButton, gbc);

    return p;
}

    private JPanel createInsertPersonPanel() {
        JPanel p = new JPanel(new GridBagLayout());
        p.setBorder(new TitledBorder("1 & 4 – Vloženie osoby"));

        GridBagConstraints gbc = baseGbc();

        personIdField = new JTextField(10);
        personIdField.setEditable(false);
        personIdField.setText("-");
        personNameField = new JTextField(10);
        personSurnameField = new JTextField(10);
        personBirthField = new JTextField("01:01:2000", 10);
        insertPersonButton = new JButton("Vložiť osobu");

        gbc.gridx = 0; gbc.gridy = 0;
        p.add(new JLabel("ID pacienta (max 10 znakov):"), gbc);
        gbc.gridx = 1;
        p.add(personIdField, gbc);

        gbc.gridx = 2;
        p.add(new JLabel("Meno:"), gbc);
        gbc.gridx = 3;
        p.add(personNameField, gbc);

        gbc.gridx = 4;
        p.add(new JLabel("Priezvisko:"), gbc);
        gbc.gridx = 5;
        p.add(personSurnameField, gbc);

        gbc.gridx = 6;
        p.add(new JLabel("Dátum narodenia (DD:MM:RRRR):"), gbc);
        gbc.gridx = 7;
        p.add(personBirthField, gbc);

        gbc.gridx = 8;
        p.add(insertPersonButton, gbc);

        return p;
    }

    private JPanel createInsertTestPanel() {
        JPanel p = new JPanel(new GridBagLayout());
        p.setBorder(new TitledBorder("1 – Vloženie PCR testu"));

        GridBagConstraints gbc = baseGbc();

        testCodeField = new JTextField(8);
        testCodeField.setEditable(false);
        testCodeField.setText("-");
        testPatientIdField = new JTextField(10);
        testDateTimeField = new JTextField("01:01:2025 12:00", 14);
        testResultCheck = new JCheckBox("Pozitívny výsledok");
        testValueField = new JTextField("0.0", 6);
        testNoteField = new JTextField(12);
        insertTestButton = new JButton("Vložiť test");

        gbc.gridx = 0; gbc.gridy = 0;
        p.add(new JLabel("Kód testu (int):"), gbc);
        gbc.gridx = 1;
        p.add(testCodeField, gbc);

        gbc.gridx = 2;
        p.add(new JLabel("ID pacienta:"), gbc);
        gbc.gridx = 3;
        p.add(testPatientIdField, gbc);

        gbc.gridx = 4;
        p.add(new JLabel("Dátum a čas:"), gbc);
        gbc.gridx = 5;
        p.add(testDateTimeField, gbc);

        gbc.gridx = 6;
        p.add(testResultCheck, gbc);

        gbc.gridx = 7;
        p.add(new JLabel("Hodnota testu (double):"), gbc);
        gbc.gridx = 8;
        p.add(testValueField, gbc);

        gbc.gridx = 9;
        p.add(new JLabel("Poznámka (max 11 znakov):"), gbc);
        gbc.gridx = 10;
        p.add(testNoteField, gbc);

        gbc.gridx = 11;
        p.add(insertTestButton, gbc);

        return p;
    }

    private JPanel createPersonSearchPanel() {
        JPanel p = new JPanel(new BorderLayout());
        p.setBorder(new TitledBorder("2 & 7 – Vyhľadanie a editácia osoby, výpis jej testov"));

        // horný panel – vyhľadanie osoby
        JPanel top = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = baseGbc();

        searchPersonIdField = new JTextField(10);
        searchPersonButton = new JButton("Nájsť osobu");

        gbc.gridx = 0; gbc.gridy = 0;
        top.add(new JLabel("ID pacienta:"), gbc);
        gbc.gridx = 1;
        top.add(searchPersonIdField, gbc);
        gbc.gridx = 2;
        top.add(searchPersonButton, gbc);

        // stred – editačné polia osoby
        JPanel center = new JPanel(new GridBagLayout());
        center.setBorder(new TitledBorder("Údaje osoby (ID sa nemení)"));

        editPersonIdField = new JTextField(10);
        editPersonIdField.setEditable(false);
        editPersonNameField = new JTextField(10);
        editPersonSurnameField = new JTextField(10);
        editPersonBirthField = new JTextField(10);
        updatePersonButton = new JButton("Uložiť zmeny osoby");

        gbc = baseGbc();
        gbc.gridx = 0; gbc.gridy = 0;
        center.add(new JLabel("ID pacienta:"), gbc);
        gbc.gridx = 1;
        center.add(editPersonIdField, gbc);

        gbc.gridx = 2;
        center.add(new JLabel("Meno:"), gbc);
        gbc.gridx = 3;
        center.add(editPersonNameField, gbc);

        gbc.gridx = 4;
        center.add(new JLabel("Priezvisko:"), gbc);
        gbc.gridx = 5;
        center.add(editPersonSurnameField, gbc);

        gbc.gridx = 6;
        center.add(new JLabel("Dátum narodenia:"), gbc);
        gbc.gridx = 7;
        center.add(editPersonBirthField, gbc);

        gbc.gridx = 8;
        center.add(updatePersonButton, gbc);

        // spodok – výpis testov osoby
        JPanel bottom = new JPanel(new BorderLayout());
        bottom.setBorder(new TitledBorder("Všetky testy osoby"));

        personTestsArea = new JTextArea();
        personTestsArea.setEditable(false);
        personTestsArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));

        JScrollPane scroll = new JScrollPane(personTestsArea);
        personTestsCountLabel = new JLabel("Počet testov: 0");

        bottom.add(scroll, BorderLayout.CENTER);
        bottom.add(personTestsCountLabel, BorderLayout.SOUTH);

        p.add(top, BorderLayout.NORTH);
        p.add(center, BorderLayout.CENTER);
        p.add(bottom, BorderLayout.SOUTH);

        return p;
    }

    private JPanel createTestSearchPanel() {
        JPanel p = new JPanel(new BorderLayout());
        p.setBorder(new TitledBorder("3, 5 & 8 – Vyhľadanie, editácia a zmazanie testu"));

        // horný panel – vyhľadanie testu
        JPanel top = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = baseGbc();

        searchTestCodeField = new JTextField(8);
        searchTestButton = new JButton("Nájsť test");

        gbc.gridx = 0; gbc.gridy = 0;
        top.add(new JLabel("Kód testu (int):"), gbc);
        gbc.gridx = 1;
        top.add(searchTestCodeField, gbc);
        gbc.gridx = 2;
        top.add(searchTestButton, gbc);

        // stred – editačné polia testu
        JPanel center = new JPanel(new GridBagLayout());
        center.setBorder(new TitledBorder("Údaje testu (kód testu sa nemení)"));

        editTestCodeField = new JTextField(8);
        editTestCodeField.setEditable(false);
        editTestPatientIdField = new JTextField(10);
        editTestDateTimeField = new JTextField(14);
        editTestResultCheck = new JCheckBox("Pozitívny výsledok");
        editTestValueField = new JTextField(6);
        editTestNoteField = new JTextField(12);
        updateTestButton = new JButton("Uložiť zmeny testu");
        deleteTestButton = new JButton("Zmazať test");

        gbc = baseGbc();
        gbc.gridx = 0; gbc.gridy = 0;
        center.add(new JLabel("Kód testu:"), gbc);
        gbc.gridx = 1;
        center.add(editTestCodeField, gbc);

        gbc.gridx = 2;
        center.add(new JLabel("ID pacienta:"), gbc);
        gbc.gridx = 3;
        center.add(editTestPatientIdField, gbc);

        gbc.gridx = 4;
        center.add(new JLabel("Dátum a čas:"), gbc);
        gbc.gridx = 5;
        center.add(editTestDateTimeField, gbc);

        gbc.gridx = 6;
        center.add(editTestResultCheck, gbc);

        gbc.gridx = 7;
        center.add(new JLabel("Hodnota:"), gbc);
        gbc.gridx = 8;
        center.add(editTestValueField, gbc);

        gbc.gridx = 9;
        center.add(new JLabel("Poznámka:"), gbc);
        gbc.gridx = 10;
        center.add(editTestNoteField, gbc);

        gbc.gridx = 11;
        center.add(updateTestButton, gbc);

        gbc.gridx = 12;
        center.add(deleteTestButton, gbc);

        p.add(top, BorderLayout.NORTH);
        p.add(center, BorderLayout.CENTER);

        return p;
    }

    private JPanel createDeletePersonPanel() {
        JPanel p = new JPanel(new GridBagLayout());
        p.setBorder(new TitledBorder("6 – Vymazanie osoby + jej testov"));

        GridBagConstraints gbc = baseGbc();

        deletePersonIdField = new JTextField(10);
        deletePersonButton = new JButton("Zmazať osobu aj s testami");

        gbc.gridx = 0; gbc.gridy = 0;
        p.add(new JLabel("ID pacienta:"), gbc);
        gbc.gridx = 1;
        p.add(deletePersonIdField, gbc);
        gbc.gridx = 2;
        p.add(deletePersonButton, gbc);

        JLabel info = new JLabel("Pozn.: Zmažú sa všetky testy tejto osoby (max 6).");
        gbc.gridx = 3;
        p.add(info, gbc);

        return p;
    }

    private JPanel createDumpPanel() {
        JPanel p = new JPanel(new BorderLayout());

        dumpTextArea = new JTextArea();
        dumpTextArea.setEditable(false);
        dumpTextArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));

        JScrollPane scroll = new JScrollPane(dumpTextArea);

        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
        dumpPersonsButton = new JButton("Dump indexu osôb");
        dumpTestsButton = new JButton("Dump indexu testov");

        top.add(dumpPersonsButton);
        top.add(dumpTestsButton);

        p.add(top, BorderLayout.NORTH);
        p.add(scroll, BorderLayout.CENTER);

        return p;
    }

    /**
     * Вкладка "Generátor" – генерация пациентов и тестов.
     */
    private JPanel createGeneratorPanel() {
        JPanel p = new JPanel();
        p.setLayout(new BoxLayout(p, BoxLayout.Y_AXIS));

        // Generovanie pacientov
        JPanel genPersonsPanel = new JPanel(new GridBagLayout());
        genPersonsPanel.setBorder(new TitledBorder("Generovanie pacientov"));

        GridBagConstraints gbc = baseGbc();

        genPersonCountField = new JTextField("10", 6);
        genPersonsButton = new JButton("Generovať pacientov");

        gbc.gridx = 0; gbc.gridy = 0;
        genPersonsPanel.add(new JLabel("Počet pacientov na vygenerovanie:"), gbc);
        gbc.gridx = 1;
        genPersonsPanel.add(genPersonCountField, gbc);
        gbc.gridx = 2;
        genPersonsPanel.add(genPersonsButton, gbc);

        // Generovanie testov
        JPanel genTestsPanel = new JPanel(new GridBagLayout());
        genTestsPanel.setBorder(new TitledBorder("Generovanie PCR testov pre existujúcich pacientov"));

        gbc = baseGbc();

        genTestCountField = new JTextField("20", 6);
        genTestsButton = new JButton("Generovať testy");

        gbc.gridx = 0; gbc.gridy = 0;
        genTestsPanel.add(new JLabel("Počet testov na vygenerovanie:"), gbc);
        gbc.gridx = 1;
        genTestsPanel.add(genTestCountField, gbc);
        gbc.gridx = 2;
        genTestsPanel.add(genTestsButton, gbc);

        p.add(genPersonsPanel);
        p.add(genTestsPanel);

        return p;
    }

    private GridBagConstraints baseGbc() {
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.anchor = GridBagConstraints.WEST;
        return gbc;
    }

    // =========================================================
    // Listener-y
    // =========================================================

    private void registerListeners() {
        // zavretie okna -> zavrieť service (súbory)
        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                service.close();
            }
        });

        // 1 & 4 – vloženie osoby
        insertPersonButton.addActionListener(e -> handleInsertPerson());

        // 1 – vloženie PCR testu
        insertTestButton.addActionListener(e -> handleInsertTest());

        // 2 & 7 – vyhľadanie a editácia osoby
        searchPersonButton.addActionListener(e -> handleSearchPerson());
        updatePersonButton.addActionListener(e -> handleUpdatePerson());

        // 3,5,8 – vyhľadanie, editácia a zmazanie testu
        searchTestButton.addActionListener(e -> handleSearchTest());
        updateTestButton.addActionListener(e -> handleUpdateTest());
        deleteTestButton.addActionListener(e -> handleDeleteTest());

        // 6 – vymazanie osoby + testov
        deletePersonButton.addActionListener(e -> handleDeletePerson());

        // Dump
        dumpPersonsButton.addActionListener(e -> handleDumpPersons());
        dumpTestsButton.addActionListener(e -> handleDumpTests());

        // Generátor
        genPersonsButton.addActionListener(e -> handleGeneratePersons());
        genTestsButton.addActionListener(e -> handleGenerateTests());
    }

    // =========================================================
    // Handlery – osoby
    // =========================================================

    private void handleInsertPerson() {
        String meno = personNameField.getText().trim();
        String priez = personSurnameField.getText().trim();
        String birth = personBirthField.getText().trim();

        if (meno.isEmpty() || priez.isEmpty() || birth.isEmpty()) {
            showError("Meno, priezvisko a dátum narodenia musia byť vyplnené.");
            return;
        }

        PatientRecord p = service.insertPersonAuto(meno, priez, birth);
        if (p == null) {
            showError("Osobu sa nepodarilo vložiť (neočakávaná chyba s ID).");
            return;
        }

        personIdField.setText(p.getId());
        showInfo("Osoba bola vložená. Pridelené ID: " + p.getId());
    }

    private void handleSearchPerson() {
        String id = searchPersonIdField.getText().trim();
        if (id.isEmpty()) {
            showError("ID pacienta nesmie byť prázdne.");
            return;
        }

        PcrDatabaseService.PersonWithTests result = service.findPersonWithTests(id);
        if (result == null) {
            showInfo("Osoba s ID " + id + " sa nenašla.");
            clearPersonEditFields();
            clearPersonTestsArea();
            return;
        }

        PatientRecord p = result.getPerson();
        List<PCRTestRecord> tests = result.getTests();

        editPersonIdField.setText(p.getId());
        editPersonNameField.setText(p.getMeno());
        editPersonSurnameField.setText(p.getPriezvisko());
        editPersonBirthField.setText(p.getDate());

        StringBuilder sb = new StringBuilder();
        int i = 1;
        for (PCRTestRecord t : tests) {
            sb.append(String.format("#%d  code=%s  dateTime=%s  value=%.3f  result=%s  note=%s%n",
                    i++,
                    t.getId(),
                    t.getDateTime(),
                    t.getValue(),
                    t.isResult() ? "POS" : "NEG",
                    t.getNote()
            ));
        }
        personTestsArea.setText(sb.toString());
        personTestsArea.setCaretPosition(0);
        personTestsCountLabel.setText("Počet testov: " + tests.size());
    }

    private void handleUpdatePerson() {
        String id = editPersonIdField.getText().trim();
        if (id.isEmpty()) {
            showError("Nie je načítaná žiadna osoba na editáciu.");
            return;
        }
        String meno = editPersonNameField.getText().trim();
        String priez = editPersonSurnameField.getText().trim();
        String birth = editPersonBirthField.getText().trim();

        if (meno.isEmpty() || priez.isEmpty() || birth.isEmpty()) {
            showError("Meno, priezvisko a dátum narodenia nesmú byť prázdne.");
            return;
        }

        PatientRecord updated = new PatientRecord(meno, priez, birth, id);
        boolean ok = service.updatePerson(updated);
        if (!ok) {
            showError("Osobu sa nepodarilo aktualizovať (ID možno neexistuje).");
        } else {
            showInfo("Údaje osoby boli aktualizované.");
        }
    }

    private void clearPersonEditFields() {
        editPersonIdField.setText("");
        editPersonNameField.setText("");
        editPersonSurnameField.setText("");
        editPersonBirthField.setText("");
    }

    private void clearPersonTestsArea() {
        personTestsArea.setText("");
        personTestsCountLabel.setText("Počet testov: 0");
    }

    // =========================================================
    // Handlery – testy
    // =========================================================

    private void handleInsertTest() {
        String patientId = testPatientIdField.getText().trim();
        String dt = testDateTimeField.getText().trim();
        boolean result = testResultCheck.isSelected();
        String valueStr = testValueField.getText().trim();
        String note = testNoteField.getText().trim();

        if (patientId.isEmpty() || dt.isEmpty() || valueStr.isEmpty()) {
            showError("ID pacienta, dátum/čas a hodnota musia byť vyplnené.");
            return;
        }

        double value;
        try {
            value = Double.parseDouble(valueStr);
        } catch (NumberFormatException ex) {
            showError("Hodnota testu musí byť číslo (double).");
            return;
        }

        PCRTestRecord t = service.insertTestAuto(patientId, dt, result, value, note);
        if (t == null) {
            showInfo("Vloženie testu zlyhalo. Možné dôvody:\n" +
                    " - neexistujúci pacient,\n" +
                    " - pacient už má 6 testov.");
            return;
        }

        testCodeField.setText(t.getId());
        showInfo("Test bol vložený. Kód testu: " + t.getId());
    }

    private void handleSearchTest() {
        String codeStr = searchTestCodeField.getText().trim();
        if (codeStr.isEmpty()) {
            showError("Kód testu nesmie byť prázdny.");
            return;
        }
        int code;
        try {
            code = Integer.parseInt(codeStr);
        } catch (NumberFormatException ex) {
            showError("Kód testu musí byť celé číslo.");
            return;
        }

        PcrDatabaseService.TestWithPerson result = service.findTestWithPerson(code);
        if (result == null) {
            showInfo("Test s kódom " + code + " sa nenašiel.");
            clearTestEditFields();
            return;
        }

        PCRTestRecord t = result.getTest();
        PatientRecord p = result.getPerson();

        editTestCodeField.setText(String.valueOf(code));
        editTestPatientIdField.setText(t.getPatientId());
        editTestDateTimeField.setText(t.getDateTime());
        editTestResultCheck.setSelected(t.isResult());
        editTestValueField.setText(String.valueOf(t.getValue()));
        editTestNoteField.setText(t.getNote());

        if (p != null) {
            showInfo(
                    "Test nájdený.\n\n" +
                    "Pacient:\n" +
                    "  ID: " + p.getId() + "\n" +
                    "  Meno: " + p.getMeno() + "\n" +
                    "  Priezvisko: " + p.getPriezvisko() + "\n" +
                    "  Dátum narodenia: " + p.getDate() + "\n"
            );
        } else {
            showInfo("Test nájdený, ale pacienta sa nepodarilo načítať.");
        }
    }

    private void handleUpdateTest() {
        String codeStr = editTestCodeField.getText().trim();
        if (codeStr.isEmpty()) {
            showError("Nie je načítaný žiadny test na editáciu.");
            return;
        }
        int code;
        try {
            code = Integer.parseInt(codeStr);
        } catch (NumberFormatException ex) {
            showError("Kód testu je neplatný.");
            return;
        }

        String patientId = editTestPatientIdField.getText().trim();
        String dt = editTestDateTimeField.getText().trim();
        boolean result = editTestResultCheck.isSelected();
        String valueStr = editTestValueField.getText().trim();
        String note = editTestNoteField.getText().trim();

        if (patientId.isEmpty() || dt.isEmpty() || valueStr.isEmpty()) {
            showError("ID pacienta, dátum/čas a hodnota nesmú byť prázdne.");
            return;
        }

        double value;
        try {
            value = Double.parseDouble(valueStr);
        } catch (NumberFormatException ex) {
            showError("Hodnota testu musí byť číslo.");
            return;
        }

        PCRTestRecord updated = new PCRTestRecord(
                dt,
                patientId,
                code,
                result,
                value,
                note
        );

        boolean ok = service.updateTest(updated);
        if (!ok) {
            showError("Test sa nepodarilo aktualizovať (možno neexistuje).");
        } else {
            showInfo("Údaje testu boli aktualizované.");
        }
    }
    
    private void handleOpenDb() {
        String pBase = dbPersonsField.getText().trim();
        String tBase = dbTestsField.getText().trim();
        int bs;

        try {
            bs = Integer.parseInt(dbBlockSizeField.getText().trim());
        } catch (Exception ex) {
            showError("Neplatná veľkosť bloku.");
            return;
        }

        service.close();
        try {
            service = new PcrDatabaseService(pBase, tBase, bs);
            showInfo("DBS otvorená: " + pBase + " / " + tBase);
        } catch (Exception ex) {
            showError("Chyba pri otváraní DBS: " + ex.getMessage());
        }
    }
    private void handleDeleteTest() {
        String codeStr = editTestCodeField.getText().trim();
        if (codeStr.isEmpty()) {
            codeStr = searchTestCodeField.getText().trim();
        }
        if (codeStr.isEmpty()) {
            showError("Zadaj kód testu na zmazanie.");
            return;
        }

        int code;
        try {
            code = Integer.parseInt(codeStr);
        } catch (NumberFormatException ex) {
            showError("Kód testu musí byť celé číslo.");
            return;
        }

        boolean ok = service.deleteTestByCode(code);
        if (!ok) {
            showInfo("Test s kódom " + code + " sa nenašiel.");
        } else {
            showInfo("Test bol zmazaný.");
            clearTestEditFields();
        }
    }

    private void clearTestEditFields() {
        editTestCodeField.setText("");
        editTestPatientIdField.setText("");
        editTestDateTimeField.setText("");
        editTestResultCheck.setSelected(false);
        editTestValueField.setText("");
        editTestNoteField.setText("");
    }

    // =========================================================
    // Handler – vymazanie osoby + testov
    // =========================================================

    private void handleDeletePerson() {
        String id = deletePersonIdField.getText().trim();
        if (id.isEmpty()) {
            showError("ID pacienta nesmie byť prázdne.");
            return;
        }

        boolean ok = service.deletePersonAndTests(id);
        if (!ok) {
            showInfo("Osoba s ID " + id + " sa nenašla.");
        } else {
            showInfo("Osoba a všetky jej testy boli zmazané.");
        }
    }

    // =========================================================
    // Dump
    // =========================================================

    private void handleDumpPersons() {
        String dump = service.dumpPersonsIndex();
        dumpTextArea.setText(dump);
        dumpTextArea.setCaretPosition(0);
    }

    private void handleDumpTests() {
        String dump = service.dumpTestsIndex();
        dumpTextArea.setText(dump);
        dumpTextArea.setCaretPosition(0);
    }

    // =========================================================
    // Generátor – handlery
    // =========================================================

    private void handleGeneratePersons() {
        String countStr = genPersonCountField.getText().trim();
        if (countStr.isEmpty()) {
            showError("Zadaj počet pacientov na generovanie.");
            return;
        }
        int count;
        try {
            count = Integer.parseInt(countStr);
        } catch (NumberFormatException ex) {
            showError("Počet pacientov musí byť celé číslo.");
            return;
        }
        if (count <= 0) {
            showError("Počet pacientov musí byť kladné číslo.");
            return;
        }

        try {
            int inserted = service.generateRandomPatients(count);
            showInfo("Vygenerovaných pacientov: " + inserted + " z požadovaných " + count + ".");
        } catch (IllegalStateException ex) {
            showError("Chyba pri generovaní pacientov: " + ex.getMessage());
        }
    }

    private void handleGenerateTests() {
        String countStr = genTestCountField.getText().trim();
        if (countStr.isEmpty()) {
            showError("Zadaj počet testov na generovanie.");
            return;
        }
        int count;
        try {
            count = Integer.parseInt(countStr);
        } catch (NumberFormatException ex) {
            showError("Počet testov musí byť celé číslo.");
            return;
        }
        if (count <= 0) {
            showError("Počet testov musí byť kladné číslo.");
            return;
        }

        try {
            int inserted = service.generateRandomTestsForExistingPatients(count);
            if (inserted == 0) {
                showInfo("Žiadny test nebol vygenerovaný. Buď nemáš žiadnych pacientov, alebo všetci už majú 6 testov.");
            } else {
                showInfo("Vygenerovaných testov: " + inserted + " z požadovaných " + count + ".");
            }
        } catch (IllegalStateException ex) {
            showError("Chyba pri generovaní testov: " + ex.getMessage());
        }
    }

    // =========================================================
    // UX pomocné метódy
    // =========================================================

    private void showError(String msg) {
        JOptionPane.showMessageDialog(this, msg, "Chyba", JOptionPane.ERROR_MESSAGE);
    }

    private void showInfo(String msg) {
        JOptionPane.showMessageDialog(this, msg, "Info", JOptionPane.INFORMATION_MESSAGE);
    }

    // =========================================================
    // main
    // =========================================================

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            PcrSystemFrame frame = new PcrSystemFrame();
            frame.setVisible(true);
        });
    }
}
