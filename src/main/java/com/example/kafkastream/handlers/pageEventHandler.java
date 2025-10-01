package com.example.kafkastream.handlers;

import com.example.kafkastream.events.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class pageEventHandler {
    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return(input)->{
            System.out.println("++++++++++++++++");
            System.out.println(input.date());
            System.out.println(input.name());
        };
    }
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return () -> new PageEvent(
                Math.random()>0.5?"P1":"P2",
                Math.random()>0.5?"U1":"U2",
                new Date(),10+new Random().nextInt(1000)
        );
    }
    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> KstreamFunction(){
        return(input)->
                input.filter((k,v)->v.duration()>100)
                        .map((k,v)->new KeyValue<>(v.name(),v.duration()));
    }
}
