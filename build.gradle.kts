plugins {
    application
}

group = "com.raftis"
version = "0.2.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // A real Redis client, used only in tests to prove compatibility.
    testImplementation("redis.clients:jedis:5.2.0")
}

// Plain Java 21, no frameworks. Compiles to Java 21 bytecode even when a newer JDK runs Gradle.
tasks.withType<JavaCompile>().configureEach {
    options.release = 21
}

application {
    mainClass = "com.raftis.Main"
}

tasks.test {
    useJUnitPlatform()
    // The crash test starts the server as a separate process and needs this classpath.
    systemProperty("raftis.serverClasspath", sourceSets["main"].runtimeClasspath.asPath)
    // Pass -Draftis.* options (crashRuns, simSeeds, simSeed...) from the command line to the tests.
    System.getProperties().stringPropertyNames()
        .filter { it.startsWith("raftis.") }
        .forEach { systemProperty(it, System.getProperty(it)) }
    testLogging {
        events("passed", "failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
