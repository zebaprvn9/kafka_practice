package com.basic.kafka.Basic_Kafka.sort;

import java.util.*;

class Student implements Comparable<Student> {

    String name;
    int rollNumber;

    Student(String name, int rollNumber) {
        this.name = name;
        this.rollNumber = rollNumber;
    }

    @Override
    public String toString() {
        return rollNumber + " - " + name;
    }

    /**
     * @param o the object to be compared.
     * @return
     */
    @Override
    public int compareTo(Student o) {
        return Integer.compare(this.rollNumber, o.rollNumber);
    }
}

public class ComparableSort {

    public static void main(String[] args) {
        Student student2 = new Student("zeba", 2);
        Student student1 = new Student("staff", 1);
        Student student3 = new Student("bold", 3);

        List<Student> students = new ArrayList<>();
        students.add(student2);
        students.add(student1);
        students.add(student3);

        //Collections.sort(students);
        //System.out.println(students);

        //Comparator<Student> studentComparator = (s1, s2) -> s1.name.compareTo(s2.name);
        Comparator<Student> studentComparator = Comparator
                .comparing((Student student) -> student.rollNumber)
                .thenComparing(s-> s.name);
        students.sort(studentComparator);
        System.out.println(students);

        students.stream().sorted(Comparator.comparing(student -> student.name)).forEach(System.out::println);

        Map<String, String> map = new HashMap<>();
        map.put("zeba", "hello");
        map.put(null, "done");
        System.out.println(map);

    }
}
