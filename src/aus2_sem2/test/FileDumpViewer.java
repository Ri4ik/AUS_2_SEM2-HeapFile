package aus2_sem2.test;

import aus2_sem2.model.PatientRecord;
import aus2_sem2.storage.HeapFile;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;
import javax.swing.*;

/**
 * GUI-окно для просмотра heap-filu,
 * generovania/mazania náhodných záznamov,
 * vloženia/zmazania konkrétnej záznamu
 * + tlačidlo "Test funkcionality" (vloží 10 a zmaže 4 z nich).
 */
public class FileDumpViewer extends JFrame {

    private final HeapFile<PatientRecord> heapFile;
    private final JTextArea textArea;

    private final JButton refreshButton;

    private final JTextField insertCountField;
    private final JButton insertButton;

    private final JLabel deleteLimitLabel;
    private final JTextField deleteCountField;
    private final JButton deleteButton;

    // поля для ВСТАВКИ/УДАЛЕНИЯ КОНКРЕТНОЙ ЗАПИСИ
    private final JTextField menoField;
    private final JTextField priezField;
    private final JTextField dateField;  // format: DD:MM:RRRR
    private final JTextField idField;
    private final JButton insertOneButton;
    private final JButton deleteByIdButton;

    // нова кнопка: тест функционала
    private final JButton testButton;

    private final Random random;

    public FileDumpViewer(HeapFile<PatientRecord> heapFile) {
        super("Heap File Dump Viewer");

        this.heapFile = heapFile;
        this.textArea = new JTextArea();

        this.refreshButton = new JButton("Refresh dump");

        this.insertCountField = new JTextField("10", 8);
        this.insertButton = new JButton("Insert random");

        this.deleteLimitLabel = new JLabel("Delete count (max 0):");
        this.deleteCountField = new JTextField("5", 8);
        this.deleteButton = new JButton("Delete random");

        this.menoField = new JTextField(10);
        this.priezField = new JTextField(10);
        this.dateField = new JTextField("01:01:2000", 10);
        this.idField = new JTextField(10);
        this.insertOneButton = new JButton("Insert one");
        this.deleteByIdButton = new JButton("Delete by ID");

        this.testButton = new JButton("Test funkcionality");

        this.random = new Random();

        initUi();
        registerListeners();
        refreshDump();
    }

    private void initUi() {
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setLayout(new BorderLayout());

        textArea.setEditable(false);
        textArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));

        JScrollPane scrollPane = new JScrollPane(textArea);
        scrollPane.setPreferredSize(new Dimension(1000, 600));

        add(scrollPane, BorderLayout.CENTER);

        JPanel controlPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.anchor = GridBagConstraints.WEST;

        // ==== ПЕРВАЯ СТРОКА: массовые вставка/удаление + refresh + test ====
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

        // ==== ВТОРАЯ СТРОКА: конкретная запись ====
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

        add(controlPanel, BorderLayout.SOUTH);

        pack();
        setLocationRelativeTo(null);
    }

    private void registerListeners() {
        refreshButton.addActionListener(e -> refreshDump());
        insertButton.addActionListener(e -> handleInsertRandom());
        deleteButton.addActionListener(e -> handleDeleteRandom());
        insertOneButton.addActionListener(e -> handleInsertOne());
        deleteByIdButton.addActionListener(e -> handleDeleteById());

        // новая логика: тест функционала
        testButton.addActionListener(e -> handleTestFunctional());

        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                heapFile.close();
            }
        });
    }

    private void refreshDump() {
        String dump = heapFile.dumpDebugInfo();
        textArea.setText(dump);
        textArea.setCaretPosition(0);
        updateDeleteLimitLabel();
    }

    private void updateDeleteLimitLabel() {
        int total = heapFile.getTotalValidRecords();
        deleteLimitLabel.setText("Delete count (max " + total + "):");
    }

    // ================== МАССОВАЯ ВСТАВКА ==================

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

        for (int i = 0; i < count; i++) {
            PatientRecord rec = generateRandomRecord();
            heapFile.insert(rec);
        }

        refreshDump();
    }

    // ================== МАССОВОЕ УДАЛЕНИЕ ==================

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

        int total = heapFile.getTotalValidRecords();
        if (count > total) {
            JOptionPane.showMessageDialog(this,
                    "В системе только " + total + " записей. Нельзя удалить " + count + ".",
                    "Limit error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        List<Long> addresses = heapFile.getAllAddresses();
        if (addresses.size() < count) {
            JOptionPane.showMessageDialog(this,
                    "Найдено только " + addresses.size() + " валидных адресов. Нельзя удалить " + count + ".",
                    "Limit error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        Collections.shuffle(addresses, random);
        for (int i = 0; i < count; i++) {
            long addr = addresses.get(i);
            heapFile.delete(addr);
        }

        refreshDump();
    }

    // ================== ВСТАВКА КОНКРЕТНОЙ ЗАПИСИ ==================

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
        heapFile.insert(rec);

        refreshDump();
    }

    // ================== УДАЛЕНИЕ КОНКРЕТНОЙ ЗАПИСИ ПО ID ==================

    private void handleDeleteById() {
        String id = idField.getText().trim();

        if (id.isEmpty()) {
            JOptionPane.showMessageDialog(this,
                    "ID pre mazanie nesmie byť prázdne.",
                    "Input error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        List<Long> addresses = heapFile.getAllAddresses();
        boolean deleted = false;

        for (long addr : addresses) {
            PatientRecord rec = heapFile.get(addr);
            if (rec != null && id.equals(rec.getId())) {
                heapFile.delete(addr);
                deleted = true;
                break; // удаляем только первую совпавшую запись
            }
        }

        if (!deleted) {
            JOptionPane.showMessageDialog(this,
                    "Záznam s ID = " + id + " sa nenašiel.",
                    "Delete",
                    JOptionPane.INFORMATION_MESSAGE);
        }

        refreshDump();
    }

    // ================== TEST FUNKCIONALITY ==================

    /**
     * Вставляет 10 записей, затем удаляет 4 из них.
     * Для наглядности: записи будут с предсказуемыми данными.
     */
    private void handleTestFunctional() {
        List<Long> addrs = new ArrayList<>();

        // 1) Вставляем 10 тестовых записей
        for (int i = 0; i < 10; i++) {
            String meno = "TestM" + i;
            String priez = "TestP" + i;
            String date = String.format("%02d:%02d:%04d", (i % 28) + 1, (i % 12) + 1, 2000 + i);
            String id = String.format("TST%07d", i);

            PatientRecord rec = new PatientRecord(meno, priez, date, id);
            long addr = heapFile.insert(rec);
            addrs.add(addr);
        }

        // 2) Удаляем 4 из них: например записи с индексами 1, 3, 5, 7 (если хватает)
        int[] toDeleteIdx = {1, 3, 5, 7};
        for (int idx : toDeleteIdx) {
            if (idx < addrs.size()) {
                long addr = addrs.get(idx);
                heapFile.delete(addr);
            }
        }

        refreshDump();

        JOptionPane.showMessageDialog(this,
                "Test funkcionality: vložených 10 záznamov, zmazané 4 z nich.",
                "Test",
                JOptionPane.INFORMATION_MESSAGE);
    }

    // ================== ГЕНЕРАЦИЯ СЛУЧАЙНОЙ ЗАПИСИ ==================

    private PatientRecord generateRandomRecord() {
        int year = 1950 + random.nextInt(60);   // 1950..2009
        int month = 1 + random.nextInt(12);     // 1..12
        int day = 1 + random.nextInt(28);       // 1..28

        String date = String.format("%02d:%02d:%04d", day, month, year);

        String meno = "M" + random.nextInt(100000);
        String priezvisko = "P" + random.nextInt(100000);
        String id = String.format("R%07d", random.nextInt(10_000_000));

        return new PatientRecord(meno, priezvisko, date, id);
    }
}
