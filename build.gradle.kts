plugins {
    kotlin("jvm") version "2.0.0"
}

group = "com.github.handrake.krocks"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.rocksdb:rocksdbjni:9.4.0")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}