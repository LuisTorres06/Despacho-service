package com.example.despachoreactive;

import com.example.despachoreactive.service.EventBus;
import com.example.despachoreactive.service.ExpiracionDespachosJob;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

class ExpiracionDespachosJobTest {
    @Test
    void puedeConstruirseConIntervaloRelojYSchedulerControlados() {
        DatabaseClient database = mock(DatabaseClient.class);
        EventBus bus = new EventBus();
        ExpiracionDespachosJob job = new ExpiracionDespachosJob(database, bus, Duration.ofSeconds(1), Clock.systemUTC(), Schedulers.immediate());

        assertThatCode(job::detener).doesNotThrowAnyException();
    }
}