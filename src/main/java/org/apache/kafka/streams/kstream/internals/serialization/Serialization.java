package org.apache.kafka.streams.kstream.internals.serialization;

import java.io.*;

public class Serialization {
    /**
     * Generic serialization function. Serializes any provided object via ObjectOutputStream.
     *
     * @param object    Object to serialize
     * @param <T>       Type of object
     * @return          Byte array serialization of the provided object.
     * @throws IOException
     */
    public static <T> byte[] toByteArray(T object) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(object);
        byte[] result = bos.toByteArray();
        oos.close();
        bos.close();
        return result;
    }

    /**
     * Generic deserialization function. Deserializes any provided byte array via ObjectInputStream.
     *
     * @param data      Byte array to deserialize
     * @param <T>       Type to deserialize to
     * @return          Deserialization of provided byte array into T
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws ClassCastException
     */
    public static <T> T fromByteArray(byte[] data, Class<T> t) throws IOException, ClassNotFoundException, ClassCastException{
        ByteArrayInputStream boi = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(boi);

        Object obj = ois.readObject();
        ois.close();
        boi.close();

        if (obj == null)
            return null;

        if (!t.isAssignableFrom(obj.getClass()))
            throw new ClassCastException("Deserialized " + obj.getClass().getName() + " can not be cast to " + t.getName());

        return (T) obj;
    }
}
