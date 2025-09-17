package com.basic.kafka.Basic_Kafka.functional_interface;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;


class User {

    private String name;
    private boolean active;
    private int salary;
    private double tax;

    public User(String name, boolean active, int salary) {
        this.name = name;
        this.active = active;
        this.salary = salary;
    }

    public String getName() {
        return name;
    }

    public boolean isActive() {
        return active;
    }

    public int getSalary() {
        return salary;
    }

    public double getTax() {
        return tax;
    }

    public boolean isUserActive() {
        return this.active;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", active=" + active +
                ", salary=" + salary +
                ", tax=" + tax +
                '}';
    }

    public void calculateTax() {
        this.tax = 0.018 * this.salary;
    }
}

class TaxReport {

    private String name;
    private int salary;
    private double tacAmount;

    public TaxReport(String name, int salary, double tacAmount) {
        this.name = name;
        this.salary = salary;
        this.tacAmount = tacAmount;
    }

    @Override
    public String toString() {
        return "TaxReport{" +
                "name='" + name + '\'' +
                ", salary=" + salary +
                ", tacAmount=" + tacAmount +
                '}';
    }
}

public class FunctionalInterfaceExample {

    /**
     * Functional Interface are interfaces
     * 1. which represent functionality
     * 2. have only single abstract function
     * 3. it also enables lambda
     *
     *
     * Predicate<T>
     * A functional interface that takes a single argument of type T and returns a boolean.
     * It is typically used for conditional checks and filtering.
     * Method signature: boolean test(T t)
     *
     * Consumer<T>
     * A functional interface that takes a single argument of type T and returns no result (void).
     * It is used to perform operations that cause side-effects (printing, updating, logging, etc.).
     * Method signature: void accept(T t)
     *
     * Function<T, R>
     * A functional interface that takes an argument of type T and returns a result of type R.
     * It is used for transforming or mapping data from one type to another.
     * Method signature: R apply(T t)
     *
     * Supplier<T>
     * A functional interface that does not take any arguments but supplies/returns an object of type T.
     * It is typically used for lazy object creation or fetching data.
     * Method signature: T get()
     *
     */
    public static void main(String[] args) {

        //supplier example
        Supplier<List<User>> userSupplier = () -> Arrays.asList(
                new User("bob", true, 2000),
                new User("peter", false, 3000),
                new User("megan", true, 4000)
        );

        // Use Supplier.get() to fetch users
        List<User> users = userSupplier.get();

        //predicate example
        Predicate<User> active = User::isUserActive;

        //consumer example
        Consumer<User> calculateTax = User::calculateTax;

        //consumer example
        Consumer<User> printUser = System.out::println;

        //consumer example with chaining
        Consumer<User> calculateTaxAndPrintUser = calculateTax.andThen(printUser);

        //using predicate and consumer both at same time
        users.stream().filter(active).forEach(calculateTaxAndPrintUser);

        //example of function
        Function<User, TaxReport> generateTaxReport = user -> {
            user.calculateTax();
            return new TaxReport(user.getName(), user.getSalary(), user.getTax());
        };

        //consumer
        Consumer<TaxReport> printTaxReport = System.out::println;

        //it has supplier, predicate, function and consumer all.
        //user came from supplier userSupplier.get(),
        //filter uses predicate as active user
        //map uses Function as generateTaxReport takes user as input and provide taxReport as output
        //forEach uses consumer as print tax report as it does not take any input but generate output.
        users.stream().filter(active).map(generateTaxReport).forEach(printTaxReport);

    }
}
