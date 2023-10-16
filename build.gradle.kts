plugins {
    java
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
}

group = "me.leavestyle"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly(group = "org.projectlombok", name = "lombok", version = "1.18.24")
    annotationProcessor(group = "org.projectlombok", name = "lombok", version = "1.18.24")
    implementation(group = "org.apache.commons", name = "commons-collections4", version = "4.4")
    implementation(group = "org.apache.commons", name = "commons-lang3", version = "3.12.0")
    implementation(group = "com.fasterxml.jackson.core", name = "jackson-databind", version = "2.12.7.1")
    implementation(group = "ch.qos.logback", name = "logback-classic", version = "1.3.5")
    implementation(group = "ch.qos.logback", name = "logback-core", version = "1.3.5")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(group = "redis.clients", name = "jedis", version = "4.2.0")
    testCompileOnly(group = "org.projectlombok", name = "lombok", version = "1.18.24")
    testAnnotationProcessor(group = "org.projectlombok", name = "lombok", version = "1.18.24")
}

tasks.test {
    useJUnitPlatform()
}