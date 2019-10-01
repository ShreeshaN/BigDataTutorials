package com.project2.spacialjoin;

import com.project2.beans.Rectangle;
import com.project2.beans.XYCoordinate;
import com.utils.GeneralUtilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGenerator {

    private Random randomGenerator = new Random();
    private int heightRange = 20;
    private int widthRange = 5;
    private int xRange = 500;
    private int yRange = 500;
    private int numberOfRectangles = 100;
    private int numberOfCoordinates = 500;
    private String COMMA = ",";


    public void isCoordinateWithinRange() {

    }


    public List<String> createRectangles() {
        List<String> rectangles = new ArrayList<>();
        for (int i = 0; i < numberOfRectangles; i++) {
            String topLeft, topRight, bottomLeft, bottomRight;

            // logic to build rectangle
            float h = GeneralUtilities.getRandomNumberGivenRange(heightRange);
            float w = GeneralUtilities.getRandomNumberGivenRange(widthRange);
            float bottomX = GeneralUtilities.getRandomNumberGivenRange(xRange);
            float bottomY = GeneralUtilities.getRandomNumberGivenRange(yRange);

            Rectangle rectangle = new Rectangle(bottomX, bottomY, h, w);
            rectangles.add(rectangle.toString());
        }
        return rectangles;

    }

    public List<String> createXYCoordinates() {
        List<String> xyCoodinates = new ArrayList<>();
        for (int i = 0; i < numberOfCoordinates; i++) {
            float x = GeneralUtilities.getRandomNumberGivenRange(xRange);
            float y = GeneralUtilities.getRandomNumberGivenRange(yRange);
            XYCoordinate xyCoordinate = new XYCoordinate(x, y);
            xyCoodinates.add(xyCoordinate.toString());
        }
        return xyCoodinates;

    }

    public static void main(String[] args) {
        String rectangleDatasetFilename = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/project2/rectangles_in_500xyspace.txt";
        String xyCoordinateDatasetFilename = "/Users/badgod/badgod_documents/github/BigDataTutorials/input/project2/xy_coordinates_in_500xyspace.txt";
        DataGenerator generator = new DataGenerator();
        System.out.println("Generating " + generator.numberOfRectangles + " rectanges");
        List<String> rectangles = generator.createRectangles();
        System.out.println("Generating " + generator.numberOfCoordinates + " coordinates");
        List<String> xyCoordinates = generator.createXYCoordinates();

        GeneralUtilities.writeIterableToFile(rectangles, rectangleDatasetFilename);
        GeneralUtilities.writeIterableToFile(xyCoordinates, xyCoordinateDatasetFilename);

    }
}


