plugins {
    kotlin("jvm") version "1.4.32"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.apache.kafka:kafka-clients:2.7.0")
    implementation("com.twitter:hbc-core:2.2.0")
}
