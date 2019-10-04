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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static Map<String, String> getListOfDivisionsForPoint(int x, int y, int radius, List<SubSpace> subSpaceList) {
        Map<String, String> divisions = new HashMap<>();
        for (int i = 0; i < subSpaceList.size(); i++) {
            SubSpace subSpace = subSpaceList.get(i);
//            if (checkIfCircleOverlapsRectangle(subSpace.getX1(), subSpace.getY1(), subSpace.getX2(), subSpace.getY2(), x, y, radius)) {
//                divisions.add(String.valueOf(i));
//            }

            if (checkIfCoordinateWithinRectangle(subSpace.getX1(), subSpace.getY1(), subSpace.getX2(), subSpace.getY2(), x, y)) {
                divisions.putIfAbsent(String.valueOf(i), x + "_" + y);
            } else {
                float width = subSpace.getX2() - subSpace.getX1();
                float height = subSpace.getY2() - subSpace.getY1();
                float distX = Math.abs(x - subSpace.getX1() - width / 2);
                float distY = Math.abs(y - subSpace.getY1() - height / 2);
//                if (distX > (width / 2 + radius)) {
//
////                System.out.println(" Point circle intersects rectangle, " + (distX > (width / 2 + radius)));
//                    return false;
//                }
//
//                if (distY > (height / 2 + radius)) {
////                System.out.println(" Point circle intersects rectangle, " + (distY > (height / 2 + radius)));
//                    return false;
//                }
                float dx = distX - width / 2;
                float dy = distY - height / 2;
                if ((distX <= (width / 2)) || (distY <= (height / 2)) || (dx * dx + dy * dy <= (radius * radius))) {
                    divisions.putIfAbsent(String.valueOf(i), x + "_" + y + "_N__orig");
                }
//                if  {
////                System.out.println(" Point circle intersects rectangle, " + (distY <= height / 2));
//                    return true;
//                }
//
//
////            System.out.println(" Point circle intersects rectangle, " + (dx * dx + dy * dy <= (radius * radius)));
//                return ;
//            }
            }
        }
        return divisions;
    }


    public static boolean checkIfCircleOverlapsRectangle(int x1, int y1, int x2, int y2, int centreX, int centreY,
                                                         int radius) {

        // if centre of circle within rectangle
//        System.out.println("checkIfCircleOverlapsRectangle " + x1 + " , " + y1 + " , " + x2 + " , " + y2 + " , " + centreX + " , " + centreY + " , " + radius);
        if (centreX >= x1 && centreX <= x2 && centreY >= y1 && centreY <= y2) {
//            System.out.println(" Point within rectangle, TRUE");
            return true;
        } else {
//            System.out.println("checkIfCircleIntersectsSubspace " + x1 + " , " + y1 + " , " + x2 + " , " + y2 + " , " + centreX + " , " + centreY + " , " + radius);
//            int rectangleDiagonal = (int) Math.sqrt(Math.pow((x2 - x1), 2) + Math.pow((y2 - y1), 2));
//            int distanceBetweenCircleAndCentreOfRectangle = (int) Math.sqrt(Math.pow(((x1 + x2) / 2) - centreX, 2) + Math.pow(((y1 + y2) / 2) - centreY, 2));
//            System.out.println("rectangle dignoal " + rectangleDiagonal);
//            System.out.println("distanceBetweenCircleAndCentreOfRectangle " + distanceBetweenCircleAndCentreOfRectangle);
//            System.out.println((rectangleDiagonal / 2) + radius <= distanceBetweenCircleAndCentreOfRectangle);
//            if ((rectangleDiagonal / 2) + radius <= distanceBetweenCircleAndCentreOfRectangle)
//                return true;
//            else
//                return false;

//            var distX = Math.abs(circle.x - rect.x - rect.w / 2);
//            var distY = Math.abs(circle.y - rect.y - rect.h / 2);
//            if (distX > (rect.w / 2 + circle.r)) {
//                return false;
//            }
//            if (distY > (rect.h / 2 + circle.r)) {
//                return false;
//            }
//            if (distX <= (rect.w / 2)) {
//                return true;
//            }
//            if (distY <= (rect.h / 2)) {
//                return true;
//            }
//            var dx = distX - rect.w / 2;
//            var dy = distY - rect.h / 2;
//            return (dx * dx + dy * dy <= (circle.r * circle.r));
            float width = x2 - x1;
            float height = y2 - y1;
            float distX = Math.abs(centreX - x1 - width / 2);
            float distY = Math.abs(centreY - y1 - height / 2);
            if (distX > (width / 2 + radius)) {

//                System.out.println(" Point circle intersects rectangle, " + (distX > (width / 2 + radius)));
                return false;
            }

            if (distY > (height / 2 + radius)) {
//                System.out.println(" Point circle intersects rectangle, " + (distY > (height / 2 + radius)));
                return false;
            }

            if (distX <= (width / 2)) {
//                System.out.println(" Point circle intersects rectangle, " + (distX <= (width / 2)));
                return true;
            }

            if (distY <= (height / 2)) {
//                System.out.println(" Point circle intersects rectangle, " + (distY <= height / 2));
                return true;
            }

            float dx = distX - width / 2;
            float dy = distY - height / 2;
//            System.out.println(" Point circle intersects rectangle, " + (dx * dx + dy * dy <= (radius * radius)));
            return (dx * dx + dy * dy <= (radius * radius));

        }
    }


    public static boolean checkIfCoordinateWithinRectangle(float bottomLeftX, float bottomLeftY, float topRightX,
                                                           float topRightY, float x, float y) {
        return x >= bottomLeftX && x <= topRightX && y >= bottomLeftY && y <= topRightY;

    }
//
//    public static boolean checkIfCircleIntersectsSubspace(int x1, int y1, int x2, int y2, int centreX, int centreY, int radius) {
//        System.out.println("checkIfCircleIntersectsSubspace " + x1 + " , " + y1 + " , " + x2 + " , " + y2 + " , " + centreX + " , " + centreY + " , " + radius);
//        int rectangleDiagonal = (int) Math.sqrt(Math.pow((x2 - x1), 2) + Math.pow((y2 - y1), 2));
//        int distanceBetweenCircleAndCentreOfRectangle = (int) Math.sqrt(Math.pow(((x1 + x2) / 2) - centreX, 2) + Math.pow(((y1 + y2) / 2) - centreY, 2));
//        System.out.println("rectangle dignoal " + rectangleDiagonal);
//        System.out.println("distanceBetweenCircleAndCentreOfRectangle " + distanceBetweenCircleAndCentreOfRectangle);
//        System.out.println((rectangleDiagonal / 2) + radius <= distanceBetweenCircleAndCentreOfRectangle);
//        if ((rectangleDiagonal / 2) + radius <= distanceBetweenCircleAndCentreOfRectangle)
//            return true;
//        else
//            return false;
//    }

    public static int getNumberOfPointsInCircle(int centreX, int centreY, int radius, List<int[]> points) {
        // if the distance between that point and radius is more than radius then point is outside circle
        int count = 0;
        for (int[] point : points) {
            if (centreX == point[0] && centreY == point[1])
                continue;
            double distanceBetweenPointAndCentre = Math.sqrt(Math.pow((centreX - point[0]), 2) + Math.pow((centreY - point[1]), 2));
            if (distanceBetweenPointAndCentre <= radius)
                count++;
        }
        return count;
    }
}
