package com.exe.impl;

import com.exe.Shape;

import java.util.Objects;

public class Circle implements Shape {
    //半径
    private double radius;

    public Circle(double radius) {
        this.radius = radius;
    }

    @Override
    public Double calArea() {
        return radius * radius;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Circle circle = (Circle) o;
        return Double.compare(circle.radius, radius) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(radius);
    }
}
