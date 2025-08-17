package com.basic.kafka.Basic_Kafka.designPattern;

interface Shape {

    void draw();
}

class Rectangle implements Shape {

    @Override
    public void draw() {
        System.out.println("This is Rectangle Shape");
    }
}

class Square implements Shape {

    @Override
    public void draw() {
        System.out.println("This is Square Shape");
    }
}

class RoundRectangle implements Shape {

    @Override
    public void draw() {
        System.out.println("This is Round Rectangle Shape");
    }
}

class RoundSquare implements Shape {

    @Override
    public void draw() {
        System.out.println("This is Round Square Shape");
    }
}

/**
 * our shape factory class
 */
public class ShapeFactory {

    public Shape getShape(String shapeType) {
        if(null == shapeType) {
            return null;
        }
        switch (shapeType.toUpperCase()) {
            case "RECTANGLE":
                return new Rectangle();
            case "SQUARE":
                return new Square();
            case "ROUND_RECTANGLE" :
                return new RoundRectangle();
            case "ROUND_SQUARE":
                return new RoundSquare();
            default:
                throw new IllegalStateException("Unknown Shape provided: " + shapeType);

        }
    }

    public static void main(String[] args) {
        AbstractFactory shapeFactory = FactoryProducer.getFactory(false);
        Shape square = shapeFactory.getShape("SQUARE");
        square.draw();

        AbstractFactory roundShapeFactory = FactoryProducer.getFactory(true);
        Shape roundSquare = roundShapeFactory.getShape("ROUND_SQUARE");
        roundSquare.draw();
    }
}

abstract class AbstractFactory {
    abstract Shape getShape(String shapeType);
}

class NormalShapeFactory extends AbstractFactory {

    @Override
    Shape getShape(String shapeType) {
        if(null == shapeType) {
            return null;
        }
        switch (shapeType.toUpperCase()) {
            case "RECTANGLE":
                return new Rectangle();
            case "SQUARE":
                return new Square();
            default:
                throw new IllegalStateException("Unknown Shape provided: " + shapeType);

        }
    }
}

class RoundShapeFactory extends AbstractFactory {
    @Override
    Shape getShape(String shapeType) {
        if(null == shapeType) {
            return null;
        }
        switch (shapeType.toUpperCase()) {
            case "ROUND_RECTANGLE":
                return new Rectangle();
            case "ROUND_SQUARE":
                return new RoundSquare();
            default:
                throw new IllegalStateException("Unknown Shape provided: " + shapeType);

        }
    }
}

class FactoryProducer {
    public static AbstractFactory getFactory(boolean rounded) {
        if(rounded) {
            return new RoundShapeFactory();
        } else {
            return new NormalShapeFactory();
        }
    }
}

