package com.ufmg.dodetl.imtu;
//
//import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
//import io.confluent.kafka.serializers.KafkaAvroDeserializer;
//import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.common.utils.Bytes;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.kstream.GlobalKTable;
//import org.apache.kafka.streams.kstream.Materialized;
//import org.apache.kafka.streams.state.KeyValueStore;
//
///**
// * Created by gusta on 16/02/2018.
// */
//public class GKTable {
//
//    void getGKTable(String topicName) {
//        StreamsBuilder builder = new StreamsBuilder();
//        GlobalKTable<GenericRecord, GenericRecord> wordCounts = builder.globalTable(
//                topicName,
//                Materialized.<GenericRecord, GenericRecord, KeyValueStore<Bytes, byte[]>>as(
//                        topicName + "_STORE" /* table/store name */)
//                        .withKeySerde(new GenericAvroSerde()) /* key serde */
//                        .withValueSerde(new GenericAvroSerde()) /* value serde */
//        );
//
//
//    }
//}
