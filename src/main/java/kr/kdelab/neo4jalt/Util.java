package kr.kdelab.neo4jalt;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

public class Util {
    public static String toString(Exception e) {
        try (StringWriter stringWriter = new StringWriter()) {
            try (PrintWriter printWriter = new PrintWriter(stringWriter)) {
                e.printStackTrace(printWriter);
            }

            return stringWriter.toString();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return "";
    }
}
