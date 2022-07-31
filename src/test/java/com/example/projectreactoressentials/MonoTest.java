package com.example.projectreactoressentials;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
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
 * 5. Publisher -> (onNext -> send to subscriber) Subscriber
 * até:
 * 1. Publisher send all the requested data.
 * 2. Publisher send all data it has. (onComplete) subscriber and subscription are canceled.
 * 3. When an error occurs. (onError) subscriber and subscription are canceled.
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

    @Test
    void monoSubscriberConsumerError() {

        String name = "Gustavo Santos";
        //Publisher
        Mono<String> mono = Mono.just(name)
            .map(s -> {
                throw new RuntimeException("Testing with error");
            });

        //                  Consumer                       errorConsumer
        mono.subscribe(s -> log.info("Value{}", s), s -> log.error("Algo deu errado"));
        mono.subscribe(s -> log.info("Value{}", s), Throwable::printStackTrace);


        log.info("----------------------------");

        //Verify
        StepVerifier
            .create(mono)//Create a publisher
            .expectError(RuntimeException.class)// Verify that the object arrived.
            .verify();

    }

    @Test
    void monoSubscriberConsumerComplete() {

        String name = "Gustavo Santos";
        //Publisher
        Mono<String> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase);

        //                  Consumer                       errorConsumer               CompleteConsumer
        mono.subscribe(s -> log.info("Value {}", s), Throwable::printStackTrace, () -> log.info("FINISHED!"));

        log.info("----------------------------");

        //Verify
        StepVerifier
            .create(mono)//Create a publisher
            .expectNext(name.toUpperCase())// Verify that the object arrived.
            .verifyComplete();

    }

    @Test
    void monoSubscriberConsumerSubscription() {

        String name = "Gustavo Santos";
        //Publisher
        Mono<String> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase);

        //                  Consumer                       errorConsumer               CompleteConsumer       ConsumerSubscription
        mono.subscribe(s -> log.info("Value {}", s), Throwable::printStackTrace, () -> log.info("FINISHED!"), subscription -> subscription.request(5));


        log.info("----------------------------");

        //Verify
        StepVerifier
            .create(mono)//Create a publisher
            .expectNext(name.toUpperCase())// Verify that the object arrived.
            .verifyComplete();

    }

    @Test
    void monoDoOnMethods() {

        String name = "Gustavo Santos";
        //Publisher
        Mono<Object> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase)
            .doOnSubscribe(subscription -> log.info("Subscribed"))
            .doOnRequest(longNumber -> log.info("Request received, starting doing something..."))
            .doOnNext(s -> log.info("Value is here. Executing doOnNext {}", s))
            .flatMap(s -> Mono.empty())
            .doOnNext(s -> log.info("Value is here. Executing doOnNext {}", s))
            .doOnSuccess(s -> log.info("doOnSuccess executed {} ", s));

        //                  Consumer                       errorConsumer               CompleteConsumer
        mono.subscribe(s -> log.info("Value {}", s), Throwable::printStackTrace, () -> log.info("FINISHED!"));

        log.info("----------------------------");


    }

    @Test
    void monoDoOnError() {

        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception error"))
            .doOnError(e -> log.error("Error message: {}", e.getMessage())).log();


        StepVerifier.create(error)
            .expectError(IllegalArgumentException.class)
            .verify();
    }

    @Test
    void monoDoOnErrorResume() {

        String name = "Gustavo Santos";

        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception error"))
            .onErrorResume(e -> {
                log.error("Inside on error resume");
                return Mono.just(name);
            })
            .onErrorReturn("EMPTY")
            .log();


        StepVerifier.create(error)
            .expectNext("EMPTY")
            .verifyComplete();
    }

    @Test
    void monoDoOnErrorReturn() {

        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception error"))
            .onErrorReturn("EMPTY")
            .log();


        StepVerifier.create(error)
            .expectNext("EMPTY")
            .verifyComplete();
    }
}
