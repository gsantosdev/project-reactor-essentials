package com.example.projectreactoressentials;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
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
 * TESTS WITH THE FLUX SUBSCRIBER PARAMETERS
 */
@Slf4j
public class FluxTest {

    @Test
    public void fluxSubscriber() {
        Flux<String> stringFlux = Flux.just("William", "Gustavo", "Flinstons", "Johnson").log();


        stringFlux.subscribe(i -> log.info("Consuming String {}", i));

        log.info("-----------------------------------");

        StepVerifier.create(stringFlux)
            .expectNext("William", "Gustavo", "Flinstons", "Johnson")
            .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbers() {
        Flux<Integer> numberFlux = Flux.range(1, 5).log();

        numberFlux.subscribe(i -> log.info("Consuming Number {}", i));

        log.info("-----------------------------------");

        StepVerifier.create(numberFlux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    public void fluxSubscriberFromList() {
        Flux<Integer> numberFlux = Flux.fromIterable(List.of(1, 2, 3, 4, 5)).log();

        numberFlux.subscribe(i -> log.info("Consuming Number {}", i));

        log.info("-----------------------------------");

        StepVerifier.create(numberFlux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersError() {
        Flux<Integer> numberFlux = Flux.fromIterable(List.of(1, 2, 3, 4, 5))
            .log()
            .map(i -> {
                if (i==4) {
                    throw new IndexOutOfBoundsException("Index error");
                }
                return i;
            });

        numberFlux.subscribe(i -> log.info("Consuming Number {}", i),
            Throwable::printStackTrace,
            () -> log.info("DONE!"),
            subscription -> subscription.request(3));

        log.info("-----------------------------------");

        StepVerifier.create(numberFlux)
            .expectNext(1, 2, 3)
            .expectError(IndexOutOfBoundsException.class)
            .verify();
    }

    @Test
    public void fluxSubscriberNumbersUglyBackpressure() {
        Flux<Integer> numberFlux = Flux.range(1, 10)
            .log();

        numberFlux.subscribe(new Subscriber<>() {

            private int count = 0;
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                subscription.request(2);
            }

            @Override
            public void onNext(Integer integer) {
                count++;
                if (count >= 2) {
                    count = 0;
                    subscription.request(2);
                }

            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {

            }
        });

        log.info("-----------------------------------");

        StepVerifier.create(numberFlux)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersNotUglyBackpressure() {
        Flux<Integer> numberFlux = Flux.range(1, 10)
            .log();

        numberFlux.subscribe(new BaseSubscriber<>() {
            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });

        log.info("-----------------------------------");

        StepVerifier.create(numberFlux)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test
    public void fluxSubscriberPrettyBackpressure() {
        Flux<Integer> numberFlux = Flux.range(1, 10)
            .log()
            .limitRate(3);

        numberFlux.subscribe(i -> log.info("Consuming Number {}", i));

        log.info("-----------------------------------");

        StepVerifier.create(numberFlux)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }


    @Test
    public void fluxSubscriberIntervalOne() throws InterruptedException {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
            .take(10)
            .log();

        interval.subscribe(i -> log.info("Number {}", i));

        Thread.sleep(3000);
    }


    /**
     * Flux interval publishes an event at each time interval
     */
    @Test
    public void fluxSubscriberIntervalTwo() {
        StepVerifier.withVirtualTime(this::createInterval)
            .expectSubscription()
            .expectNoEvent(Duration.ofHours(1))
            .thenAwait(Duration.ofDays(1))//Simula que passou um dia
            .expectNext(0L)
            .thenAwait(Duration.ofDays(1))
            .expectNext(1L)
            .thenCancel()
            .verify();
    }

    private Flux<Long> createInterval() {
        return Flux.interval(Duration.ofDays(1L))//Publica evento a cada 1 dia
            .log();
    }


}

