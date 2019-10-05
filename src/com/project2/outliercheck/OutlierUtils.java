/**
 * @created on: 8/4/19,
 * @author: Shreesha N,
 * @version: v0.0.1
 * @system name: badgod
 * Description:
 * <p>
 * ..todo::
 */

package com.project2.outliercheck;

import java.util.ArrayList;
import java.util.List;

public class OutlierUtils {

    public static List<SubSpace> divideSampleSpaceBasedOnRadius(int xRange, int yRange, int dividerX, int dividerY) {
        List<SubSpace> subSpaces = new ArrayList<>();
        for (int j = 0; j < xRange; ) {
            int next_j = j + dividerX;
            for (int i = 0; i < yRange; ) {

                // checking for subspaces which grow out of xy plane
                int next_i = i + dividerY;
                subSpaces.add(new SubSpace(i, j, next_i, next_j));
                i = next_i;
            }
            j = next_j;
        }
        return subSpaces;
    }

    public static List<String> getListOfDivisionsForPoint(double x, double y, double radius, List<SubSpace> subSpaceList) {
        List<String> divisions = new ArrayList<>();
        for (int i = 0; i < subSpaceList.size(); i++) {
            SubSpace subSpace = subSpaceList.get(i);

            if (checkIfCoordinateWithinRectangle(subSpace.getX1(), subSpace.getY1(), subSpace.getX2(), subSpace.getY2(), x, y)) {
                divisions.add(x + "_" + y);
            } else {
                double newX1 = subSpace.getX1() - radius;
                double newY1 = subSpace.getY1() - radius;
                double newX2 = subSpace.getX2() - radius;
                double newY2 = subSpace.getY2() - radius;
                if ((newX1 <= x) && (x <= newX2) && (newY1 <= y) && (y <= newY2))
                    // indicating this is not a original point. a.k.a boundary point
                    divisions.add(x + "_" + y + "_N__orig");
            }
        }
        return divisions;
    }


    public static boolean checkIfCoordinateWithinRectangle(float bottomLeftX, float bottomLeftY, float topRightX,
                                                           float topRightY, double x, double y) {
        return x >= bottomLeftX && x <= topRightX && y >= bottomLeftY && y <= topRightY;

    }


    public static int getNumberOfPointsInCircle(double centreX, double centreY, double radius, List<double[]> points) {
        // if the distance between that point and radius is more than radius then point is outside circle
        int count = 0;
        for (double[] point : points) {
            if (centreX == point[0] && centreY == point[1])
                continue;
            double distanceBetweenPointAndCentre = Math.sqrt(Math.pow((centreX - point[0]), 2) + Math.pow((centreY - point[1]), 2));
            if (distanceBetweenPointAndCentre <= radius)
                count++;
        }
        return count;
    }
}
