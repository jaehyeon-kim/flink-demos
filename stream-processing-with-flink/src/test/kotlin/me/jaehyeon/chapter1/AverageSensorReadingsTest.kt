package me.jaehyeon.chapter1

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

/**
 * A simple unit test to verify that Flink classes are available on the test classpath.
 */
class AverageSensorReadingsTest {
    @Test
    fun `test that Flink StreamExecutionEnvironment can be created`() {
        // This is the core of the test.
        // We are trying to use a class from the 'flink-streaming-java' dependency,
        // which is marked as 'compileOnly' in our build.gradle.kts.
        // If the test classpath was not configured correctly, this line would fail
        // with a ClassNotFoundException.
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // A simple assertion to confirm that the environment object was created successfully.
        assertNotNull(env, "The Flink execution environment should not be null.")
    }
}
