package aus2_sem2.test;

import aus2_sem2.controller.PatientFileController;
import aus2_sem2.model.PatientRecord;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import javax.swing.*;

/**
 * GUI pre zobrazenie a testovanie heap súboru pacientov.
 * View komunikuje iba cez PatientFileController.
 */
public class FileDumpViewer extends JFrame {

    private final PatientFileController controller;   // prepojenie na logiku (controller)
    private final JTextArea textArea;                 // výpis dumpu súboru

    private final JButton refreshButton;

    private final JTextField insertCountField;
    private final JButton insertButton;

    private final JLabel deleteLimitLabel;
    private final JTextField deleteCountField;
    private final JButton deleteButton;

    private final JTextField menoField;
    private final JTextField priezField;
    private final JTextField dateField;
    private final JTextField idField;
    private final JButton insertOneButton;
    private final JButton deleteByIdButton;

    private final JButton testButton;

    private final JTextField uniqueCountField;
    private final JButton insertRandomUniqueButton;
    private final JButton insertOneUniqueButton;

    public FileDumpViewer(PatientFileController controller) {
        super("Heap File Dump Viewer");
        this.controller = controller;

        this.textArea = new JTextArea();

        this.refreshButton = new JButton("Refresh dump");

        this.insertCountField = new JTextField("10", 6);
        this.insertButton = new JButton("Insert random");

        this.deleteLimitLabel = new JLabel("Delete count (max 0):");
        this.deleteCountField = new JTextField("5", 6);
        this.deleteButton = new JButton("Delete random");

        this.menoField = new JTextField(10);
        this.priezField = new JTextField(10);
        this.dateField = new JTextField("01:01:2000", 10);
        this.idField = new JTextField(10);
        this.insertOneButton = new JButton("Insert one");
        this.deleteByIdButton = new JButton("Delete by ID");

        this.testButton = new JButton("Test funkcionality");

        this.uniqueCountField = new JTextField("5", 6);
        this.insertRandomUniqueButton = new JButton("Insert random UNIQUE");
        this.insertOneUniqueButton = new JButton("Insert one UNIQUE");

        initUi();           // inicializácia vzhľadu GUI
        registerListeners(); // pripojenie handlerov na tlačidlá
        refreshDump();      // prvotné načítanie dumpu
    }

    /** Nastavenie layoutu, panelov a komponentov. */
    private void initUi() {
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setLayout(new BorderLayout());

        textArea.setEditable(false);
        textArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));

        JScrollPane scrollPane = new JScrollPane(textArea);
        scrollPane.setPreferredSize(new Dimension(1100, 650));

        add(scrollPane, BorderLayout.CENTER);

        JPanel controlPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.anchor = GridBagConstraints.WEST;

        // 1. riadok: obyčajné random insert/delete + refresh + test
        gbc.gridy = 0;
        gbc.gridx = 0;
        controlPanel.add(new JLabel("Insert count:"), gbc);

        gbc.gridx++;
        controlPanel.add(insertCountField, gbc);

        gbc.gridx++;
        controlPanel.add(insertButton, gbc);

        gbc.gridx++;
        controlPanel.add(Box.createHorizontalStrut(10), gbc);

        gbc.gridx++;
        controlPanel.add(deleteLimitLabel, gbc);

        gbc.gridx++;
        controlPanel.add(deleteCountField, gbc);

        gbc.gridx++;
        controlPanel.add(deleteButton, gbc);

        gbc.gridx++;
        controlPanel.add(Box.createHorizontalStrut(10), gbc);

        gbc.gridx++;
        controlPanel.add(refreshButton, gbc);

        gbc.gridx++;
        controlPanel.add(Box.createHorizontalStrut(10), gbc);

        gbc.gridx++;
        controlPanel.add(testButton, gbc);

        // 2. riadok: konkrétna jedna obyčajná insert + delete by ID
        gbc.gridy = 1;
        gbc.gridx = 0;
        controlPanel.add(new JLabel("Meno:"), gbc);

        gbc.gridx++;
        controlPanel.add(menoField, gbc);

        gbc.gridx++;
        controlPanel.add(new JLabel("Priezvisko:"), gbc);

        gbc.gridx++;
        controlPanel.add(priezField, gbc);

        gbc.gridx++;
        controlPanel.add(new JLabel("Date (DD:MM:RRRR):"), gbc);

        gbc.gridx++;
        controlPanel.add(dateField, gbc);

        gbc.gridx++;
        controlPanel.add(new JLabel("ID:"), gbc);

        gbc.gridx++;
        controlPanel.add(idField, gbc);

        gbc.gridx++;
        controlPanel.add(insertOneButton, gbc);

        gbc.gridx++;
        controlPanel.add(deleteByIdButton, gbc);

        // 3. riadok: UNIKÁTNE vkladanie
        gbc.gridy = 2;
        gbc.gridx = 0;
        controlPanel.add(new JLabel("Unique random count:"), gbc);

        gbc.gridx++;
        controlPanel.add(uniqueCountField, gbc);

        gbc.gridx++;
        controlPanel.add(insertRandomUniqueButton, gbc);

        gbc.gridx++;
        controlPanel.add(Box.createHorizontalStrut(10), gbc);

        gbc.gridx++;
        controlPanel.add(insertOneUniqueButton, gbc);

        add(controlPanel, BorderLayout.SOUTH);

        pack();
        setLocationRelativeTo(null);
    }

    /** Registrácia action listenerov pre tlačidlá a okno. */
    private void registerListeners() {
        refreshButton.addActionListener(e -> refreshDump());
        insertButton.addActionListener(e -> handleInsertRandom());
        deleteButton.addActionListener(e -> handleDeleteRandom());
        insertOneButton.addActionListener(e -> handleInsertOne());
        deleteByIdButton.addActionListener(e -> handleDeleteById());
        testButton.addActionListener(e -> handleTestFunctional());
        insertRandomUniqueButton.addActionListener(e -> handleInsertRandomUnique());
        insertOneUniqueButton.addActionListener(e -> handleInsertOneUnique());

        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                // HeapFile sa zatvára v main-e, tu netreba nič riešiť
            }
        });
    }

    /** Načíta aktuálny dump z controlleru a obnoví textové okno + label pre mazanie. */
    private void refreshDump() {
        String dump = controller.getDump();
        textArea.setText(dump);
        textArea.setCaretPosition(0);
        updateDeleteLimitLabel();
    }

    /** Aktualizuje text labelu pre max. počet zmazaní podľa počtu záznamov. */
    private void updateDeleteLimitLabel() {
        int total = controller.getTotalRecords();
        deleteLimitLabel.setText("Delete count (max " + total + "):");
    }

    // --- obyčajný random insert ---

    /** Handler pre vloženie náhodných záznamov (bez unikátnej kontroly). */
    private void handleInsertRandom() {
        String text = insertCountField.getText().trim();
        int count;
        try {
            count = Integer.parseInt(text);
        } catch (NumberFormatException ex) {
            JOptionPane.showMessageDialog(this,
                    "Insert count must be an integer.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        if (count <= 0) {
            JOptionPane.showMessageDialog(this,
                    "Insert count must be > 0.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        controller.insertRandomRecords(count);
        refreshDump();
    }

    // --- obyčajné random delete ---

    /** Handler pre náhodné mazanie záznamov. */
    private void handleDeleteRandom() {
        String text = deleteCountField.getText().trim();
        int count;
        try {
            count = Integer.parseInt(text);
        } catch (NumberFormatException ex) {
            JOptionPane.showMessageDialog(this,
                    "Delete count must be an integer.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        if (count <= 0) {
            JOptionPane.showMessageDialog(this,
                    "Delete count must be > 0.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        int removed = controller.deleteRandomRecords(count);
        if (removed < count) {
            JOptionPane.showMessageDialog(this,
                    "Zmazaných " + removed + " z " + count + ".",
                    "Info",
                    JOptionPane.INFORMATION_MESSAGE);
        }

        refreshDump();
    }

    // --- obyčajná jedna insert ---

    /** Handler pre vloženie jednej konkrétnej (neunikátnej) pacientskej karty. */
    private void handleInsertOne() {
        String meno = menoField.getText().trim();
        String priez = priezField.getText().trim();
        String date = dateField.getText().trim();
        String id = idField.getText().trim();

        if (meno.isEmpty() || priez.isEmpty() || date.isEmpty() || id.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                    "Meno, priezvisko, dátum a ID nesmú byť prázdne.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        if (date.length() != 10 || date.charAt(2) != ':' || date.charAt(5) != ':') {
            JOptionPane.showMessageDialog(this,
                    "Dátum musí byť vo formáte DD:MM:RRRR.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        PatientRecord rec = new PatientRecord(meno, priez, date, id);
        controller.insertRecord(rec);
        refreshDump();
    }

    // --- delete by ID ---

    /** Handler pre zmazanie záznamu podľa ID pacienta. */
    private void handleDeleteById() {
        String id = idField.getText().trim();

        if (id.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                    "ID pre mazanie nesmie byť prázdne.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        boolean deleted = controller.deleteById(id);
        if (!deleted) {
            JOptionPane.showMessageDialog(this,
                    "Záznam s ID = " + id + " sa nenašiel.",
                    "Delete",
                    JOptionPane.INFORMATION_MESSAGE);
        }

        refreshDump();
    }

    // --- test funkcionality (10 insert, 4 delete) ---

    /** Spustí interný test funkcionality v controlleri. */
    private void handleTestFunctional() {
        controller.runFunctionalTest();
        refreshDump();
        JOptionPane.showMessageDialog(this,
                "Test funkcionality: vložených 10 záznamov (obyčajne), zmazané 4 z nich.",
                "Test",
                JOptionPane.INFORMATION_MESSAGE);
    }

    // --- UNIQUE random insert ---

    /** Handler pre náhodné vkladanie záznamov s kontrolou unikátneho ID. */
    private void handleInsertRandomUnique() {
        String text = uniqueCountField.getText().trim();
        int count;
        try {
            count = Integer.parseInt(text);
        } catch (NumberFormatException ex) {
            JOptionPane.showMessageDialog(this,
                    "Unique count must be an integer.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        if (count <= 0) {
            JOptionPane.showMessageDialog(this,
                    "Unique count must be > 0.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        int inserted = controller.insertRandomUniqueRecords(count);
        if (inserted < count) {
            JOptionPane.showMessageDialog(this,
                    "Unikátne vložených " + inserted + " z požadovaných " + count + ".",
                    "Info",
                    JOptionPane.INFORMATION_MESSAGE);
        }

        refreshDump();
    }

     // --- UNIQUE jedna insert ---

    /** Handler pre vloženie jednej konkrétnej pacientky s auto-increment unikátnym ID. */
    private void handleInsertOneUnique() {
        String meno = menoField.getText().trim();
        String priez = priezField.getText().trim();
        String date = dateField.getText().trim();
        // ID pole sa pri UNIQUE režime ignoruje – generuje sa automaticky
        // String id = idField.getText().trim();

        if (meno.isEmpty() || priez.isEmpty() || date.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                    "Meno, priezvisko a dátum nesmú byť prázdne (ID sa generuje automaticky).",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        if (date.length() != 10 || date.charAt(2) != ':' || date.charAt(5) != ':') {
            JOptionPane.showMessageDialog(this,
                    "Dátum musí byť vo formáte DD:MM:RRRR.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        // ID dáme prázdne, controller ho nahradí auto-increment hodnotou
        PatientRecord rec = new PatientRecord(meno, priez, date, "");
        long addr = controller.insertRecordUnique(rec);
        if (addr == -1L) {
            // v tejto verzii sa to prakticky nestane, ID je vždy unikátne z počítadla
            JOptionPane.showMessageDialog(this,
                    "Pacient s týmto ID už existuje.",
                    "Duplicitné ID",
                    JOptionPane.ERROR_MESSAGE);
        }

        refreshDump();
    }
}
