import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    kotlin("jvm") version "2.2.20"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "me.jaehyeon"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Flink Dependencies
    implementation("org.apache.flink:flink-streaming-java:1.20.1")
    implementation("org.apache.flink:flink-clients:1.20.1")
    // Logging
    implementation("org.slf4j:slf4j-simple:2.0.17")
    // Kotlin test
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set(project.findProperty("mainClass")?.toString() ?: "me.jaehyeon.chapter1.AverageSensorReadings")
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
