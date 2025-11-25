import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.testing.Test

plugins {
    kotlin("jvm") version "2.2.20"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "me.jaehyeon"
version = "1.0-SNAPSHOT"

val localRunClasspath by configurations.creating {
    extendsFrom(configurations.implementation.get(), configurations.compileOnly.get(), configurations.runtimeOnly.get())
}

repositories {
    mavenCentral()
}

dependencies {
    // Flink Dependencies
    compileOnly("org.apache.flink:flink-streaming-java:1.20.1")
    compileOnly("org.apache.flink:flink-clients:1.20.1")
    // 'testImplementation' makes Flink available for test source compilation and execution.
    testImplementation("org.apache.flink:flink-streaming-java:1.20.1")
    testImplementation("org.apache.flink:flink-clients:1.20.1")
    // Logging
    implementation("org.slf4j:slf4j-simple:2.0.17")
    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.14.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.14.1")
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set(
        project.findProperty("mainClass")?.toString()
            ?: "me.jaehyeon.chapter1.AverageSensorReadings",
    )
}

tasks.named<JavaExec>("run") {
    // Classpath = All library dependencies + The application's compiled code.
    classpath = localRunClasspath + sourceSets.main.get().output
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.withType<ShadowJar> {
    archiveBaseName.set(rootProject.name)
    archiveClassifier.set("")
    archiveVersion.set("1.0")
    mergeServiceFiles()
}

tasks.named("build") {
    dependsOn("shadowJar")
}
