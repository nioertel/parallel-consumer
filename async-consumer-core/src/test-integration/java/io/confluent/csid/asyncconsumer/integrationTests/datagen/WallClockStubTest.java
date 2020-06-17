package io.confluent.csid.asyncconsumer.integrationTests.datagen;

import io.confluent.csid.asyncconsumer.integrationTests.GenUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class WallClockStubTest {

  @Test
  void test() {
    WallClockStub clock = new WallClockStub(GenUtils.randomSeedInstant);
    assertThat(clock.getNow()).isEqualTo(GenUtils.randomSeedInstant);
    Duration time = ofMillis(1);
    Instant nowAndAdvance = clock.advanceAndGet(time);
    assertThat(nowAndAdvance).isEqualTo(GenUtils.randomSeedInstant.plus(time));
  }
}
