/**
@created on: 18/3/19,
@author: Shreesha N,
@version: v0.0.1
@system name: badgod
Description:

..todo::

*/

package com.project2.beans;

import com.utils.StringConstants;

public class Rectangle {
    private float h;
    private float w;
    private float bottomX;
    private float bottomY;

    public Rectangle(float bottomX, float bottomY, float h, float w) {
        this.h = h;
        this.w = w;
        this.bottomX = bottomX;
        this.bottomY = bottomY;
    }

    public static boolean checkIfRectangleWithinWindow(Rectangle rectangle, float wX1, float wY1, float wX2, float wY2) {
        float bottomLeftX = rectangle.bottomX;
        float bottomLeftY = rectangle.bottomY;
        float topRightX = bottomLeftX + rectangle.w;
        float topRightY = bottomLeftY + rectangle.h;
        // (check if rectangle besides window ) || (check if rectangle is above or below window)
        if ((bottomLeftX > wX2 || topRightX < wX1) || (bottomLeftY > wY2 || topRightY < wY1))
            return false;
        return true;

    }

    @Override
    public String toString() {
        return bottomX + StringConstants.COMMA + bottomY + StringConstants.COMMA + h + StringConstants.COMMA + w;
    }

    public float getH() {
        return h;
    }

    public void setH(float h) {
        this.h = h;
    }

    public float getW() {
        return w;
    }

    public void setW(float w) {
        this.w = w;
    }

    public float getBottomX() {
        return bottomX;
    }

    public void setBottomX(float bottomX) {
        this.bottomX = bottomX;
    }

    public float getBottomY() {
        return bottomY;
    }

    public void setBottomY(float bottomY) {
        this.bottomY = bottomY;
    }
}
