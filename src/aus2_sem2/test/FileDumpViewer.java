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
 * GUI pre zobrazenie a testovanie:
 *  - HeapFile<PatientRecord> (1. záložka)
 *  - LinHashFile<PatientRecord> (2. záložka, index nad tým istým HeapFile)
 *
 * View komunikuje iba cez PatientFileController.
 */
public class FileDumpViewer extends JFrame {

    private final PatientFileController controller;   // prepojenie na logiku (controller)

    // --- komponenty pre záložku HeapFile ---
    private final JTextArea heapTextArea;

    private final JButton refreshHeapButton;

    private final JLabel deleteLimitLabel;
    private final JTextField deleteCountField;
    private final JButton deleteButton;

    private final JTextField menoField;
    private final JTextField priezField;
    private final JTextField dateField;
    private final JTextField idField;           // používa sa len na mazanie podľa ID
    private final JButton deleteByIdButton;

    private final JButton testButton;

    private final JTextField uniqueCountField;
    private final JButton insertRandomUniqueButton;
    private final JButton insertOneUniqueButton;

    // --- komponenty pre záložku LinHashFile ---
    private final JTextArea linTextArea;
    private final JButton refreshLinButton;

    private final JTextField linIdField;
    private final JButton linFindButton;
    private final JButton linDeleteButton;

    public FileDumpViewer(PatientFileController controller) {
        super("Heap File / LinHashFile Viewer");
        this.controller = controller;

        this.heapTextArea = new JTextArea();
        this.linTextArea = new JTextArea();

        this.refreshHeapButton = new JButton("Refresh heap dump");

        this.deleteLimitLabel = new JLabel("Delete count (max 0):");
        this.deleteCountField = new JTextField("5", 6);
        this.deleteButton = new JButton("Delete random");

        this.menoField = new JTextField(10);
        this.priezField = new JTextField(10);
        this.dateField = new JTextField("01:01:2000", 10);
        this.idField = new JTextField(10);
        this.deleteByIdButton = new JButton("Delete by ID");

        this.testButton = new JButton("Test funkcionality");

        this.uniqueCountField = new JTextField("5", 6);
        this.insertRandomUniqueButton = new JButton("Insert random UNIQUE");
        this.insertOneUniqueButton = new JButton("Insert one UNIQUE");

        this.refreshLinButton = new JButton("Refresh LinHash dump");
        this.linIdField = new JTextField(10);
        this.linFindButton = new JButton("Find (LinHash)");
        this.linDeleteButton = new JButton("Delete (LinHash)");

        initUi();            // inicializácia vzhľadu GUI
        registerListeners(); // pripojenie handlerov na tlačidlá
        refreshHeapDump();   // prvotné načítanie heap dumpu
        refreshLinHashDump(); // prvotné načítanie lin hash dumpu
    }

    /** Nastavenie layoutu, panelov a komponentov vrátane záložiek. */
    private void initUi() {
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setLayout(new BorderLayout());

        JTabbedPane tabbedPane = new JTabbedPane();

        // --- záložka 1: HeapFile ---
        JPanel heapPanel = createHeapPanel();
        tabbedPane.addTab("HeapFile", heapPanel);

        // --- záložka 2: LinHashFile ---
        JPanel linPanel = createLinHashPanel();
        tabbedPane.addTab("LinHashFile", linPanel);

        add(tabbedPane, BorderLayout.CENTER);

        pack();
        setLocationRelativeTo(null);
    }

    /** Vytvorí panel pre záložku HeapFile. */
    private JPanel createHeapPanel() {
        JPanel mainPanel = new JPanel(new BorderLayout());

        heapTextArea.setEditable(false);
        heapTextArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));

        JScrollPane scrollPane = new JScrollPane(heapTextArea);
        scrollPane.setPreferredSize(new Dimension(1100, 650));

        mainPanel.add(scrollPane, BorderLayout.CENTER);

        JPanel controlPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.anchor = GridBagConstraints.WEST;

        // 1. riadok: UNIQUE random insert + delete random + refresh + test
        gbc.gridy = 0;
        gbc.gridx = 0;
        controlPanel.add(new JLabel("Unique random count:"), gbc);

        gbc.gridx++;
        controlPanel.add(uniqueCountField, gbc);

        gbc.gridx++;
        controlPanel.add(insertRandomUniqueButton, gbc);

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
        controlPanel.add(refreshHeapButton, gbc);

        gbc.gridx++;
        controlPanel.add(Box.createHorizontalStrut(10), gbc);

        gbc.gridx++;
        controlPanel.add(testButton, gbc);

        // 2. riadok: vloženie jednej pacientky s auto-ID + mazanie podľa ID
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
        controlPanel.add(insertOneUniqueButton, gbc);

        gbc.gridx++;
        controlPanel.add(new JLabel("ID pre mazanie:"), gbc);

        gbc.gridx++;
        controlPanel.add(idField, gbc);

        gbc.gridx++;
        controlPanel.add(deleteByIdButton, gbc);

        mainPanel.add(controlPanel, BorderLayout.SOUTH);

        return mainPanel;
    }

    /** Vytvorí panel pre záložku LinHashFile. */
    private JPanel createLinHashPanel() {
        JPanel mainPanel = new JPanel(new BorderLayout());

        linTextArea.setEditable(false);
        linTextArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));

        JScrollPane scrollPane = new JScrollPane(linTextArea);
        scrollPane.setPreferredSize(new Dimension(1100, 650));

        mainPanel.add(scrollPane, BorderLayout.CENTER);

        JPanel controlPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.anchor = GridBagConstraints.WEST;

        // 1. riadok: refresh LinHash dump
        gbc.gridy = 0;
        gbc.gridx = 0;
        controlPanel.add(refreshLinButton, gbc);

        // 2. riadok: Find/Delete podľa ID cez LinHash
        gbc.gridy = 1;
        gbc.gridx = 0;
        controlPanel.add(new JLabel("ID:"), gbc);

        gbc.gridx++;
        controlPanel.add(linIdField, gbc);

        gbc.gridx++;
        controlPanel.add(linFindButton, gbc);

        gbc.gridx++;
        controlPanel.add(linDeleteButton, gbc);

        mainPanel.add(controlPanel, BorderLayout.SOUTH);

        return mainPanel;
    }

    /** Registrácia action listenerov pre tlačidlá a okno. */
    private void registerListeners() {
        // heap tab
        refreshHeapButton.addActionListener(e -> refreshHeapDump());
        deleteButton.addActionListener(e -> handleDeleteRandom());
        deleteByIdButton.addActionListener(e -> handleDeleteById());
        testButton.addActionListener(e -> handleTestFunctional());
        insertRandomUniqueButton.addActionListener(e -> handleInsertRandomUnique());
        insertOneUniqueButton.addActionListener(e -> handleInsertOneUnique());

        // lin hash tab
        refreshLinButton.addActionListener(e -> refreshLinHashDump());
        linFindButton.addActionListener(e -> handleLinFindById());
        linDeleteButton.addActionListener(e -> handleLinDeleteById());

        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                // korektné zatvorenie controlleru (zavrie aj súbory)
                controller.close();
            }
        });
    }

    // --- HeapFile: pomocné metódy GUI ---

    /** Načíta aktuálny dump z controlleru a obnoví textové okno + label pre mazanie. */
    private void refreshHeapDump() {
        String dump = controller.getDump();
        heapTextArea.setText(dump);
        heapTextArea.setCaretPosition(0);
        updateDeleteLimitLabel();
    }

    /** Aktualizuje text labelu pre max. počet zmazaní podľa počtu záznamov. */
    private void updateDeleteLimitLabel() {
        int total = controller.getTotalRecords();
        deleteLimitLabel.setText("Delete count (max " + total + "):");
    }

    /** Handler pre náhodné vkladanie záznamov s unikátnym auto-ID. */
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

        refreshHeapDump();
        refreshLinHashDump();
    }

    /** Handler pre vloženie jednej konkrétnej pacientky/pacienta s auto-ID. */
    private void handleInsertOneUnique() {
        String meno = menoField.getText().trim();
        String priez = priezField.getText().trim();
        String date = dateField.getText().trim();

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
        controller.insertRecordUnique(rec);

        refreshHeapDump();
        refreshLinHashDump();
    }

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

        refreshHeapDump();
        refreshLinHashDump();
    }

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

        refreshHeapDump();
        refreshLinHashDump();
    }

    /** Spustí interný test funkcionality v controlleri. */
    private void handleTestFunctional() {
        controller.runFunctionalTest();
        refreshHeapDump();
        refreshLinHashDump();
        JOptionPane.showMessageDialog(this,
                "Test funkcionality: vložených 10 záznamov (auto-ID, LinHash), zmazané 4 z nich.",
                "Test",
                JOptionPane.INFORMATION_MESSAGE);
    }

    // --- LinHashFile: pomocné metódy GUI ---

    /** Načíta aktuálny LinHash dump z controlleru a obnoví textové okno. */
    private void refreshLinHashDump() {
        String dump = controller.getLinHashDump();
        linTextArea.setText(dump);
        linTextArea.setCaretPosition(0);
    }

    /** Handler pre findById cez LinHash index. */
    private void handleLinFindById() {
        String id = linIdField.getText().trim();
        if (id.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                    "ID pre vyhľadávanie nesmie byť prázdne.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        PatientRecord rec = controller.linFindById(id);
        if (rec == null) {
            JOptionPane.showMessageDialog(this,
                    "LinHash: záznam s ID = " + id + " sa nenašiel.",
                    "Find (LinHash)",
                    JOptionPane.INFORMATION_MESSAGE);
        } else {
            JOptionPane.showMessageDialog(this,
                    "LinHash: nájdený záznam:\n" + rec.toString(),
                    "Find (LinHash)",
                    JOptionPane.INFORMATION_MESSAGE);
        }
    }

    /** Handler pre deleteById cez LinHash index. */
    private void handleLinDeleteById() {
        String id = linIdField.getText().trim();
        if (id.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                    "ID pre mazanie nesmie byť prázdne.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        boolean deleted = controller.linDeleteById(id);
        if (!deleted) {
            JOptionPane.showMessageDialog(this,
                    "LinHash: záznam s ID = " + id + " sa nenašiel.",
                    "Delete (LinHash)",
                    JOptionPane.INFORMATION_MESSAGE);
        } else {
            JOptionPane.showMessageDialog(this,
                    "LinHash: záznam s ID = " + id + " bol zmazaný.",
                    "Delete (LinHash)",
                    JOptionPane.INFORMATION_MESSAGE);
        }

        refreshHeapDump();
        refreshLinHashDump();
    }
}
