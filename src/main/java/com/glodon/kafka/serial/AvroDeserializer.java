package com.glodon.kafka.serial;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;
import java.util.Map;
@Slf4j
public class AvroDeserializer<User extends SpecificRecordBase> implements Deserializer<User>  {

    protected final Class<User> targetType;

    public AvroDeserializer(Class<User> targetType) {
        this.targetType = targetType;
    }
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @SuppressWarnings("unchecked")
    @Override
    public User deserialize(String topic, byte[] data) {
        try {
            User result = null;

            if (data != null) {
                log.debug("data='{}'", DatatypeConverter.printHexBinary(data));

                DatumReader<GenericRecord> datumReader =
                        new SpecificDatumReader<>(targetType.newInstance().getSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);

                result = (User) datumReader.read(null, decoder);
                log.debug("deserialized data='{}'", result);
            }
            return result;
        } catch (Exception ex) {
            throw new SerializationException(
                    "Can't deserialize data '" + Arrays.toString(data) + "' from topic '" + topic + "'", ex);
        }
    }
    @Override
    public void close() {

    }
}
