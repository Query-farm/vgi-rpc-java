plugins {
    `java-library`
}

val junitVersion = "5.11.3"
val testcontainersVersion = "1.21.4"

dependencies {
    api(project(":vgirpc"))
    implementation(platform("com.google.cloud:libraries-bom:26.52.0"))
    implementation("com.google.cloud:google-cloud-storage")
}

val integrationTest by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += output + compileClasspath
}

configurations[integrationTest.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get()
)
configurations[integrationTest.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get()
)

dependencies {
    integrationTest.implementationConfigurationName(platform("org.junit:junit-bom:$junitVersion"))
    integrationTest.implementationConfigurationName("org.junit.jupiter:junit-jupiter")
    integrationTest.implementationConfigurationName("org.testcontainers:junit-jupiter:$testcontainersVersion")
    integrationTest.implementationConfigurationName("com.github.luben:zstd-jni:1.5.6-8")
    integrationTest.runtimeOnlyConfigurationName("org.junit.platform:junit-platform-launcher")
    integrationTest.runtimeOnlyConfigurationName("org.slf4j:slf4j-simple:2.0.16")
}

tasks.register<Test>("integrationTest") {
    description = "Runs GCS integration tests against a pinned fake-gcs-server container."
    group = "verification"
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
}
