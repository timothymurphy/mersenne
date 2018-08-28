/**
 * Represent a Task object for a Mersenne prime number search.
 *
 * @date 2018-05-29
 * @version 1.0
 * @author Timothy Murphy
 * @author Thyago Mota (advisor)
 */

import java.io.*;

public class Task implements Serializable {

    private int     exponent;
    private boolean prime;

    public Task(int exponent) {
        this.exponent = exponent;
        prime = false;
    }

    public int getExponent() {
        return exponent;
    }

    public void setExponent(int exponent) {
        this.exponent = exponent;
    }

    public boolean isPrime() {
        return prime;
    }

    public void setPrime(boolean prime) {
        this.prime = prime;
    }

    public static byte[] serialize(Task task) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(task);
        return bos.toByteArray();
    }

    public static Task deserialize(byte bytes[]) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(bis);
        return (Task) in.readObject();
    }

    @Override
    public String toString() {
        return "Task{" +
                "exponent=" + exponent +
                ", prime=" + prime +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        Task other = (Task) obj;
        return getExponent() == other.getExponent();
    }
}
