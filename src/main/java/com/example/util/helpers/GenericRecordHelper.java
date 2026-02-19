package com.example.util.helpers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class GenericRecordHelper {

    private static GenericRecord setFieldNameAndValue(GenericRecord record, Object o) {
        Field[] fields = o.getClass().getDeclaredFields();
        for (Field field : fields) {
            try {
                // To access private fields, you must make them accessible
                field.setAccessible(true);

                // Skip any internal schema names
                if(field.getName().contains("Schema")) {
                    continue;
                }

                // Get the name of the field
                String name = field.getName();

                // Get the value of the field for the specific object instance
                // For static fields, pass null as the object argument
                Object value = field.get(Modifier.isStatic(field.getModifiers()) ? null : o);
                record.put(name, value);
            } catch (IllegalAccessException e) {
                System.out.println("Error accessing field: " + field.getName());
                e.printStackTrace();
            }
        }
        return record;
    }

    public static GenericRecord fromObjectToGenericRecord(Object o, Schema schema) {
        GenericRecord gr = new GenericData.Record(schema);
        setFieldNameAndValue(gr, o);
        return gr;
    }
}
