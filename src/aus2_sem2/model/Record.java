package aus2_sem2.model;

/**
 * Základné rozhranie pre všetky typy záznamov ukladaných v heap súbore.
 * Definuje povinné metódy pre prácu so serializáciou a identifikáciou záznamu.
 */
public interface Record {

    /** Fixná veľkosť záznamu v bajtoch. */
    int getSize();

    /** Konverzia záznamu na pevné pole bajtov. */
    byte[] toByteArray();

    /** Načítanie záznamu z pevného poľa bajtov. */
    void fromByteArray(byte[] data);

    /** Porovnanie dvoch záznamov podľa obsahu. */
    boolean equals(Record other);

    /** Unikátne ID záznamu (používa sa pri unikátnom vkladaní). */
    String getId();
}
