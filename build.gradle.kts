plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "io.github.yoonryeol.bitcoinrealtime.streaming"
version = "1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
    google()
}

dependencies {
    // Flink Core (Flink 1.19.1) — compileOnly: 컨테이너가 제공
    compileOnly("org.apache.flink:flink-streaming-java:1.19.1") {
        exclude(group = "org.lz4", module = "lz4-java")
        exclude(group = "at.yawk.lz4", module = "lz4-java")
    }
    compileOnly("org.apache.flink:flink-clients:1.19.1") {
        exclude(group = "org.lz4", module = "lz4-java")
        exclude(group = "at.yawk.lz4", module = "lz4-java")
    }
    compileOnly("org.apache.flink:flink-runtime:1.19.1") {
        exclude(group = "org.lz4", module = "lz4-java")
        exclude(group = "at.yawk.lz4", module = "lz4-java")
    }
    compileOnly("org.apache.flink:flink-json:1.19.1")

    // Flink Kafka Connector — implementation: fat JAR에 포함 필수
    implementation("org.apache.flink:flink-connector-kafka:3.3.0-1.19") {
        exclude(group = "org.lz4", module = "lz4-java")
        exclude(group = "at.yawk.lz4", module = "lz4-java")
    }
    implementation("org.apache.kafka:kafka-clients:3.8.0")

    // SnakeYAML for config
    implementation("org.yaml:snakeyaml:2.3")

    // Jackson for JSON
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.18.2")

    // Test
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("org.apache.flink:flink-test-utils-junit:1.19.1")
    testImplementation("org.assertj:assertj-core:3.26.3")
    // Flink core for test (compileOnly는 test scope에 포함되지 않음)
    testImplementation("org.apache.flink:flink-streaming-java:1.19.1")
}

configurations.all {
    resolutionStrategy {
        // Force lz4-java version to resolve conflict
        force("org.lz4:lz4-java:1.8.0")
    }
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
    }
}

application {
    mainClass = "io.github.yoonryeol.bitcoinrealtime.streaming.UpbitTradeJob"
}

tasks.named<Jar>("jar") {
    manifest {
        attributes(
            "Main-Class" to "io.github.yoonryeol.bitcoinrealtime.streaming.UpbitTradeJob"
        )
    }
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveClassifier.set("all")
    mergeServiceFiles()
    configurations = listOf(project.configurations["runtimeClasspath"])
    manifest {
        attributes(
            "Main-Class" to "io.github.yoonryeol.bitcoinrealtime.streaming.UpbitTradeJob"
        )
    }
}
