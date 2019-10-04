/**
 * @created on: 18/3/19,
 * @author: Shreesha N,
 * @version: v0.0.1
 * @system name: badgod
 * Description:
 * <p>
 * ..todo::
 */

package com.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;

public class GeneralUtilities {
    private static Random random = new Random();

    public static int getRandomNumberBetweenRange(int low, int high) {
        return random.nextInt(high - low) + low;
    }

    public static float getRandomNumberGivenRange(int range) {
        return random.nextInt(range) + 1;
    }

    public static void writeIterableToFile(Iterable listOfObjects, String filename) {
        /**
         * Use this method to write iterable to a file
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

    public static void writeIterableToFileHDFS(Iterable listOfObjects, String filename, FileSystem fs) throws IOException, URISyntaxException {
        /**
         * Use this method to write iterable to a file - HDFS
         */

        Path file = new Path(filename);
        if (fs.exists(file)) {
            fs.delete(file, true);
        }
        OutputStream os = fs.create(file);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        for (Object s : listOfObjects) {
            br.write(s.toString() + "\n");

        }
        br.close();
    }

    public static List<String> readFileIntoIterable(String filename) throws IOException {
        /**
         * Use this method to read file
         */
        File file = new File(filename);
        List<String> lines = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(file));

        String st;
        while ((st = br.readLine()) != null)
            lines.add(st.trim());
        return lines;
    }

    public static List<String> readFileIntoIterableHDFS(String filename, FileSystem fs) throws IOException {
        /**
         * Use this method to read file
         */
        List<String> lines = new ArrayList<>();
        String line = "";
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(filename))));
        while ((line = reader.readLine()) != null) {
            lines.add(line.trim());
        }
        return lines;
    }

    public static void removeDuplicates(String filename, FileSystem fs) throws IOException {
        /**
         * Use this method to read file
         */
        Path file = new Path(filename);

        // read
        Set<String> lines = new HashSet<>();
        String line = "";
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file)));
        while ((line = reader.readLine()) != null) {
            lines.add(line.trim());
        }

        // write
        if (fs.exists(file)) {
            fs.delete(file, true);
        }
        OutputStream os = fs.create(file);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        for (Object s : lines) {
            br.write(s.toString() + "\n");
        }
        br.close();
    }

    public static boolean checkIfCoordinateWithinRectangle(float bottomLeftX, float bottomLeftY, float topRightX, float topRightY, float x, float y) {
        return x >= bottomLeftX && x <= topRightX && y >= bottomLeftY && y <= topRightY;

    }

    public static boolean compareLists(List<String> l1, List<String> l2) {
        l1.replaceAll(String::trim);
        l2.replaceAll(String::trim);
        Collections.sort(l1);
        Collections.sort(l2);
        return l1.equals(l2);
    }
}
