package com.example.projectreactoressentials;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;


/**
 * Reactive Streams
 * 1. Asynchronous
 * 2. Non-blocking
 * 3. Backpressure
 * <p>
 * Interfaces
 * 1. Publisher <- (subscribe) Subscriber
 * 2. Subscription is created
 * 3. Publisher <- onSubscribe with subscription <- Publisher
 * 4. Subscription <- (request with N objects) Subscription
 * 5. Publisher -> (onNext) Subscriber
 * até:
 * 1. Publisher send all the requested data.
 * 2. Publisher send all data it has. (onComplete) subscriber and subscription are canceled.
 * 3. Quando tem um erro. (onError) subscriber and subscription are canceled.
 */

/**
 * TESTS WITH THE SUBSCRIBER PARAMETERS
 */
@Slf4j
class MonoTest {

    @Test
    void monoSubscriber() {

        String name = "Gustavo Santos";
        //Publisher
        Mono<String> mono = Mono.just(name).log();

        mono.subscribe();

        log.info("----------------------------");

        //Verify
        StepVerifier
                .create(mono)//Create a publisher
                .expectNext(name)// Verify that the object arrived.
                .verifyComplete();

    }

    @Test
    void monoSubscriberConsumer() {

        String name = "Gustavo Santos";
        //Publisher
        Mono<String> mono = Mono.just(name).log();

        //                   Consumer
        mono.subscribe(s -> log.info("Value {}", s));

        log.info("----------------------------");

        //Verify
        StepVerifier
                .create(mono)//Create a publisher
                .expectNext(name)// Verify that the object arrived.
                .verifyComplete();

    }


}
