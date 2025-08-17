package com.basic.kafka.Basic_Kafka.designPattern;

public enum SingletonEnum {
    INSTANCE;

    private String value;

    SingletonEnum() {
        this.value = "Default Value";
    }

    public static SingletonEnum getInstance() {
        return INSTANCE;
    }

    public void showValue() {
        System.out.println("value: " + value);
    }

    public void setValue(String value) {
        this.value = value;
    }

    public static void main(String[] args) {
        SingletonEnum singletonEnum = SingletonEnum.getInstance();
        singletonEnum.showValue();
        singletonEnum.setValue("Zeba");
        singletonEnum.showValue();
    }
}
