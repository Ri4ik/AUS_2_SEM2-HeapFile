package aus2_sem2;

import aus2_sem2.model.PatientRecord;
import aus2_sem2.storage.HeapFile;
import aus2_sem2.test.FileDumpViewer;

import javax.swing.SwingUtilities;

/**
 * Hlavná trieda aplikácie AUS2_SEM2.
 * Pri štarte:
 *  - len otvorí/alebo vytvorí heap súbor,
 *  - spustí GUI (FileDumpViewer),
 *  - nič automaticky nevkladá ani nemaže.
 */
public class AUS2_SEM2 {

    public static void main(String[] args) {
        String filePath = "heapfile_patients.dat";
        int clusterSizeBytes = 256;

        // NIKDY automaticky nevymazávame súbor, aby sa dalo pracovať viacnásobne
        HeapFile<PatientRecord> heapFile = new HeapFile<>(filePath, clusterSizeBytes, PatientRecord.class);

        SwingUtilities.invokeLater(() -> {
            FileDumpViewer viewer = new FileDumpViewer(heapFile);
            viewer.setVisible(true);
        });
    }
}
