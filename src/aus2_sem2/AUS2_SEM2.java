package aus2_sem2;

import aus2_sem2.model.PatientRecord;
import aus2_sem2.storage.HeapFile;
import aus2_sem2.test.FileDumpViewer;

import java.util.ArrayList;
import java.util.List;
import javax.swing.SwingUtilities;

/**
 * Hlavná trieda aplikácie AUS2_SEM2.
 * Demonštruje prácu s HeapFile<PatientRecord>:
 * - vytváranie heap súboru
 * - vkladanie záznamov
 * - čítanie záznamov
 * - mazanie záznamov
 * - zobrazenie obsahu cez GUI FileDumpViewer
 */
public class AUS2_SEM2 {

    public static void main(String[] args) {
        // cesta k dátovému súboru (relatívna k pracovného adresáru)
        String filePath = "heapfile_patients.dat";

        // veľkosť bloku/clusteru na disku (napr. 4096 bajtov)
        int clusterSizeBytes = 4096;

        // zoznam adries vložených záznamov, aby sme mohli testovať get/delete
        List<Long> addresses = new ArrayList<>();

        // vytvorenie / otvorenie heap súboru
        HeapFile<PatientRecord> heapFile = new HeapFile<>(filePath, clusterSizeBytes, PatientRecord.class);

        // 1) vkladanie niekoľkých testovacích záznamov
        System.out.println("Vkladám testovacie záznamy...");

        for (int i = 0; i < 10; i++) {
            String meno = "Meno" + i;
            String priezvisko = "Priez" + i;
            int year = 1990 + (i % 10);
            int month = (i % 12) + 1;
            int day = (i % 28) + 1;
            String id = String.format("ID%07d", i);

            PatientRecord rec = new PatientRecord(meno, priezvisko, year, month, day, id);
            long addr = heapFile.insert(rec);
            addresses.add(addr);

            System.out.println("  Inserted: " + rec.toString() + " at address=" + addr);
        }

        // 2) čítanie niekoľkých záznamov pomocou get()
        System.out.println("\nOverujem get() pre niektoré adresy...");
        for (int i = 0; i < addresses.size(); i += 3) {
            long addr = addresses.get(i);
            PatientRecord rec = heapFile.get(addr);
            System.out.println("  Get address=" + addr + " -> " + (rec != null ? rec.toString() : "null"));
        }

        // 3) testovanie delete() pre niektoré adresy
        System.out.println("\nTestujem delete() pre niektoré adresy...");
        for (int i = 1; i < addresses.size(); i += 4) {
            long addr = addresses.get(i);
            boolean deleted = heapFile.delete(addr);
            System.out.println("  Delete address=" + addr + " -> " + deleted);
        }

        // 4) opätovné čítanie po mazaniach
        System.out.println("\nOverujem get() po delete...");
        for (int i = 0; i < addresses.size(); i++) {
            long addr = addresses.get(i);
            PatientRecord rec = heapFile.get(addr);
            System.out.println("  After delete, get address=" + addr + " -> " + (rec != null ? rec.toString() : "null"));
        }

        // 5) výpis debug informácií v konzole
        System.out.println("\n--- DEBUG DUMP ---");
        String dump = heapFile.dumpDebugInfo();
        System.out.println(dump);

        // 6) spustenie jednoduchého GUI na zobrazenie obsahu heap súboru
        SwingUtilities.invokeLater(() -> {
            FileDumpViewer viewer = new FileDumpViewer(heapFile);
            viewer.setVisible(true);
        });
    }
}
