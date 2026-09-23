plugins {
    `java-library`
}

val awsSdkVersion = "2.28.29"
val junitVersion = "5.11.3"
val testcontainersVersion = "1.21.4"

dependencies {
    api(project(":vgirpc"))
    implementation(platform("software.amazon.awssdk:bom:$awsSdkVersion"))
    // Arrow 19 pulls netty-common/netty-buffer 4.2; the AWS SDK pulls the rest of Netty
    // at 4.1. Mixing Netty minors is unsupported, so pin every module to Arrow's.
    implementation(platform("io.netty:netty-bom:4.2.9.Final"))
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:s3-transfer-manager")
    implementation("software.amazon.awssdk:regions")
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
    description = "Runs S3 integration tests against a pinned RustFS container."
    group = "verification"
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
}
