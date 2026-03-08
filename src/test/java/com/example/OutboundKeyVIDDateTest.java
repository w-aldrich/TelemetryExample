package com.example;

import com.example.model.outbound.OutboundKeyVIDDate;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OutboundKeyVIDDateTest {

    @Test
    public void testConstructor() {
        OutboundKeyVIDDate key;
        try {
            key = new OutboundKeyVIDDate("1", 1L);
        } catch (IOException e) {
            assert(false);
        }
        assert (true);
    }

    @Test
    public void testToGenericRecord() {
        OutboundKeyVIDDate key = null;
        String id = "id";
        long date = 1;
        try {
            key = new OutboundKeyVIDDate(id, date);
        } catch (IOException e) {
            assert(false);
        }

        GenericRecord out = key.toGenericRecord();
        assertEquals(out.get("vehicleId").toString(), id);
        assertEquals(Long.parseLong(out.get("date").toString()), date);
    }
}
