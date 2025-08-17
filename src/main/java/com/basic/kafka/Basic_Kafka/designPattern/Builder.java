package com.basic.kafka.Basic_Kafka.designPattern;


// Product Class
class User {
    // Required parameters
    private final String firstName;
    private final String lastName;
    // Optional parameters
    private final int age;
    private final String email;
    private final String phone;
    private final boolean newsletter;

    // Private constructor
    private User(Builder builder) {
        this.firstName = builder.firstName;
        this.lastName = builder.lastName;
        this.age = builder.age;
        this.email = builder.email;
        this.phone = builder.phone;
        this.newsletter = builder.newsletter;
    }

    // Builder Class
    public static class Builder {
        // Required parameters
        private final String firstName;
        private final String lastName;

        // Optional parameters
        private int age;
        private String email;
        private String phone;
        private boolean newsletter;

        public Builder(String firstName, String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public Builder age(int age) {
            this.age = age;
            return this;
        }

        public Builder email(String email) {
            this.email = email;
            return this;
        }

        public Builder phone(String phone) {
            this.phone = phone;
            return this;
        }

        public Builder newsletter(boolean newsletter) {
            this.newsletter = newsletter;
            return this;
        }

        public User build() {
            return new User(this);
        }
    }

    @Override
    public String toString() {
        return "User [firstName=" + firstName + ", lastName=" + lastName +
                ", age=" + age + ", email=" + email + ", phone=" + phone +
                ", newsletter=" + newsletter + "]";
    }

    public static void main(String[] args) {
        User user = new Builder("Zeba", "Parveen").age(30).email("zebaprvn9@gmail.com").build();
        System.out.println(user.toString());
    }
}
