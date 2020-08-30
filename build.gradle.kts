import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val coroutinesVersion = "1.3.9"
val jupiterVersion = "5.6.2"
val logbackVersion = "1.2.3"

plugins {
    kotlin("jvm") version "1.4.0"
    application
    id("com.adarshr.test-logger") version "2.1.0"
}

group = "me.barak"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}
dependencies {
    testImplementation(kotlin("test-junit5"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${coroutinesVersion}")
    implementation("ch.qos.logback:logback-classic:${logbackVersion}")
    implementation("io.github.microutils:kotlin-logging:1.8.3")
    testImplementation("org.junit.jupiter:junit-jupiter-api:${jupiterVersion}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${jupiterVersion}")

}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}
tasks.test {
    useJUnitPlatform()
}

testlogger {
    showStandardStreams = true
}

application {
    mainClassName = "org.barak.raft.MainKt"
    applicationDefaultJvmArgs = listOf("-Dkotlinx.coroutines.debug")
}
