package aus2_sem2.util;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ByteUtils {

    // фиксированная строка -> байты фиксированной длины
    public static byte[] toFixedBytes(String s, int length) {
        byte[] result = new byte[length];
        byte[] raw = s.getBytes(StandardCharsets.UTF_8);

        int copyLen = Math.min(raw.length, length);
        System.arraycopy(raw, 0, result, 0, copyLen);

        // если строка меньше, остаток будет нулями
        return result;
    }

    // чтение строки фиксированной длины
    public static String fromFixedBytes(byte[] data, int offset, int length) {
        byte[] slice = Arrays.copyOfRange(data, offset, offset + length);

        int realLength = 0;
        for (int i = 0; i < slice.length; i++) {
            if (slice[i] != 0)
                realLength = i + 1;
        }

        return new String(slice, 0, realLength, StandardCharsets.UTF_8);
    }
}
