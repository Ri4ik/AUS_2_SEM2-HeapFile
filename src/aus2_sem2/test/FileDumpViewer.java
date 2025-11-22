package aus2_sem2.test;

import aus2_sem2.model.PatientRecord;
import aus2_sem2.storage.HeapFile;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import javax.swing.*;

/**
 * Простое GUI-окно для просмотра содержимого heap-файла.
 * Показывает результат HeapFile.dumpDebugInfo() в JTextArea.
 */
public class FileDumpViewer extends JFrame {

    private final HeapFile<PatientRecord> heapFile;
    private final JTextArea textArea;
    private final JButton refreshButton;

    /**
     * Конструктор.
     *
     * @param heapFile открытый HeapFile, который нужно отображать
     */
    public FileDumpViewer(HeapFile<PatientRecord> heapFile) {
        super("Heap File Dump Viewer");

        this.heapFile = heapFile;
        this.textArea = new JTextArea();
        this.refreshButton = new JButton("Refresh dump");

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
        scrollPane.setPreferredSize(new Dimension(800, 600));

        add(scrollPane, BorderLayout.CENTER);
        add(refreshButton, BorderLayout.SOUTH);

        pack();
        setLocationRelativeTo(null);
    }

    private void registerListeners() {
        refreshButton.addActionListener(e -> refreshDump());

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
    }
}
