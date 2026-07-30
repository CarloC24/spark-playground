package playground;

import java.io.Serializable;
import java.util.Objects;

/**
 * A person record, as a JavaBean.
 *
 * <p>This is deliberately NOT a Java record. Spark's {@code Encoders.bean} discovers
 * fields through {@link java.beans.Introspector}, which looks for {@code getX()} /
 * {@code setX()} pairs. A record exposes {@code x()} accessors and has no setters, so
 * the introspector finds no properties and the encoder fails. The mutable bean below
 * is the price of a typed {@code Dataset<PersonBean>} in Java — the Scala side gets the
 * same thing from a one-line case class.
 */
public class PersonBean implements Serializable {

    private static final long serialVersionUID = 1L;

    private long id;
    private String name;
    private String email;
    private int age;
    private String city;

    /** Required by the bean encoder, which instantiates then calls setters. */
    public PersonBean() {}

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "PersonBean{id=%d, name=%s, email=%s, age=%d, city=%s}"
                .formatted(id, name, email, age, city);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PersonBean other)) {
            return false;
        }
        return id == other.id
                && age == other.age
                && Objects.equals(name, other.name)
                && Objects.equals(email, other.email)
                && Objects.equals(city, other.city);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, email, age, city);
    }
}
