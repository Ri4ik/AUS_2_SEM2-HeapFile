package aus2_sem2;

import aus2_sem2.controller.PatientFileController;
import aus2_sem2.model.PatientRecord;
import aus2_sem2.storage.HeapFile;
import aus2_sem2.test.FileDumpViewer;

import javax.swing.SwingUtilities;

/**
 * Hlavná trieda aplikácie – spustenie modelu, controlleru a GUI.
 */
public class AUS2_SEM2 {

    public static void main(String[] args) {
        // cesta k dátovému súboru na disku
        String filePath = "heapfile_patients.dat";

        // veľkosť jedného diskového bloku
        int clusterSizeBytes = 256;

        // model – neutriedený súbor so záznamami pacientov
        HeapFile<PatientRecord> heapFile =
                new HeapFile<>(filePath, clusterSizeBytes, PatientRecord.class);

        // controller – všetka logika nad súborom
        PatientFileController controller = new PatientFileController(heapFile);

        // GUI – spustenie v EDT vlákne
        SwingUtilities.invokeLater(() -> {
            FileDumpViewer viewer = new FileDumpViewer(controller);
            viewer.setVisible(true);
        });
    }
}
