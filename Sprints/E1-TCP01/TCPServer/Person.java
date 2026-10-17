package tcp01;

import java.io.Serializable;

public class Person implements Serializable {
    private String name;
    private int year;
    private Place place;
    private static final long serialVersionUID = 1L;

    public Person(String name, int year, Place place) {
        this.name = name;
        this.year = year;
        this.place = place;
    }

    public String getName() {
        return name;
    }

    public int getYear() {
        return year;
    }

    public Place getPlace() {
        return place;
    }
}