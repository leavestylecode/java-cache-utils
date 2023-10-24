plugins {
    java
    `java-library`
    `maven-publish`
    signing
}

group = "io.github.leavestyle-coder"
version = "1.0.1"

java {
    withJavadocJar()
    withSourcesJar()
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

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

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }
            pom {
                name = "Java Cache Utils"
                description = "Convenient Utilities for Cache of Java"
                url = "https://github.com/leavestyle-coder/java-cache-utils"
                licenses {
                    license {
                        name = "GNU Lesser General Public License"
                        url = "https://www.gnu.org/licenses/lgpl-3.0.txt"
                    }
                }
                developers {
                    developer {
                        id = "leavestyle"
                        name = "Rambo"
                        email = "leavestyle101@gmail.com"
                    }
                }
                scm {
                    connection = "https://github.com/leavestyle-coder/java-cache-utils.git"
                    developerConnection = "https://github.com/leavestyle-coder/java-cache-utils.git"
                    url = "https://github.com/leavestyle-coder/java-cache-utils"
                }
            }
        }
    }
    repositories {
        maven {
            url = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
            credentials {
                username = project.properties["ossrhUsername"]?.toString()
                password = project.properties["ossrhPassword"]?.toString()
            }
        }
    }
}

signing {
    sign(publishing.publications["mavenJava"])
}

tasks.javadoc {
    if (JavaVersion.current().isJava9Compatible) {
        (options as StandardJavadocDocletOptions).addBooleanOption("html5", true)
    }
}
