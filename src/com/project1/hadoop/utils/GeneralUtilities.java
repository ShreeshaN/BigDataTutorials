package com.project1.hadoop.utils;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

public class GeneralUtilities {
    private static Random random = new Random();

    public static int getRandomNumberBetweenRange(int low, int high) {
        return random.nextInt(high - low) + low;
    }

    public static void writeIterableToFile(Iterable listOfObjects, String filename) {
        /**
         * Use this methood to write iterable to a file
         */
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(filename, "UTF-8");
            for (Object o : listOfObjects) {
                writer.println(o.toString());
            }
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                writer.close();
            }

        }
    }
}
