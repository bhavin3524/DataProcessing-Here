package util;

import java.text.MessageFormat;
import java.util.ResourceBundle;

public class MessageUtilTest {
    private static final ResourceBundle BUNDLE =
            ResourceBundle.getBundle("messages_test");

    private MessageUtilTest() {
    }

    public static String get(String key) {
        return BUNDLE.getString(key);
    }

    public static String get(String key, Object... args) {
        return MessageFormat.format(BUNDLE.getString(key), args);
    }
}
