package com.basic.kafka.Basic_Kafka.lambda;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

class Employee {
    private String name;
    private int salary;
    private boolean isActive;

    public Employee(String name, int salary, boolean isActive) {
        this.name = name;
        this.salary = salary;
        this.isActive = isActive;
    }

    public String getName() {
        return name;
    }

    public int getSalary() {
        return salary;
    }

    public boolean isActive() {
        return isActive;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "name='" + name + '\'' +
                ", salary=" + salary +
                ", isActive=" + isActive +
                '}';
    }
}

public class HighestSalaryEmployee {
    public static void main(String[] args) {
        Employee employee1 = new Employee("bob", 2000, true);
        Employee employee2 = new Employee("peter", 5000, false);
        Employee employee3 = new Employee("piere", 8000, true);

        List<Employee> employeeList = Arrays.asList(employee1, employee2, employee3);
        List<Employee> filteredEmployee = employeeList.stream().sorted(Comparator.comparingInt(Employee::getSalary).reversed())
                .filter(Employee::isActive)
                .collect(Collectors.toList());
        filteredEmployee.forEach(System.out::println);
    }

}
