plugins {
    `java-library`
    `java-test-fixtures`
}

val arrowVersion = "18.1.0"
val jettyVersion = "12.0.16"
val jacksonVersion = "2.18.2"
val slf4jVersion = "2.0.16"

dependencies {
    api("org.apache.arrow:arrow-vector:$arrowVersion")
    api("org.apache.arrow:arrow-memory-netty:$arrowVersion")
    api("org.slf4j:slf4j-api:$slf4jVersion")

    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:$jacksonVersion")
    implementation("com.github.luben:zstd-jni:1.5.6-8")

    // OAuth JWT / JWKS support lives in the optional `vgirpc-oauth` module so
    // core callers don't pull ~500KB of nimbus-jose-jwt when they don't need JWT.

    // HTTP transport (server + client)
    implementation("org.eclipse.jetty:jetty-server:$jettyVersion")
    implementation("org.eclipse.jetty:jetty-util:$jettyVersion")
    implementation("org.eclipse.jetty.ee10:jetty-ee10-servlet:$jettyVersion")
    implementation("org.eclipse.jetty:jetty-client:$jettyVersion")

    // Logging backend (only required for tests/worker; library users may bring their own)
    runtimeOnly("org.slf4j:slf4j-simple:$slf4jVersion")

    testImplementation(platform("org.junit:junit-bom:5.11.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // Shared test helpers (HttpRequestStub) consumed by vgirpc-oauth tests too.
    testFixturesApi("jakarta.servlet:jakarta.servlet-api:6.0.0")
}

// --- Multi-release JAR: Java 21 baseline + Java 22 FFM shared-memory overlay ---
//
// The core targets Java 21. The only java.lang.foreign code (FfmShm + the real
// ShmFactory) lives in the `java22` source set, packaged under
// META-INF/versions/22 and loaded only on a JDK >= 22 runtime. On Java 21 the
// baseline ShmFactory returns null and the worker uses the inline (pipe)
// transport. FFM is GA in 22, so the overlay needs no --enable-preview.

val java22 by sourceSets.creating {
    java.setSrcDirs(listOf("src/main/java22"))
}
val java22Test by sourceSets.creating {
    java.setSrcDirs(listOf("src/test/java22"))
}

configurations["java22Implementation"].extendsFrom(
    configurations["api"], configurations["implementation"]
)
configurations["java22TestImplementation"].extendsFrom(configurations["testImplementation"])
configurations["java22TestRuntimeOnly"].extendsFrom(configurations["testRuntimeOnly"])

dependencies {
    // The overlay extends the 21 baseline classes (FfmShm extends Shm).
    "java22Implementation"(sourceSets["main"].output)
    "java22TestImplementation"(sourceSets["main"].output)
    "java22TestImplementation"(java22.output)
}

// Baseline (core + its tests + fixtures) at release 21; the overlay sets at 22.
// Overrides the root's project-wide release(25). Toolchain stays JDK 25 (builds
// both targets); only the *runtime* JDK gates whether the overlay loads.
listOf("compileJava", "compileTestJava", "compileTestFixturesJava").forEach { name ->
    tasks.named<JavaCompile>(name) { options.release.set(21) }
}
listOf("compileJava22Java", "compileJava22TestJava").forEach { name ->
    tasks.named<JavaCompile>(name) { options.release.set(22) }
}

tasks.named<Jar>("jar") {
    manifest { attributes("Multi-Release" to "true") }
    into("META-INF/versions/22") { from(java22.output) }
}

// FFM tests carry Java 22 bytecode and load FfmShm, so they only run on >= 22.
// Kept out of the main `test` task so that task can run on a Java 21 runtime.
val java22TestTask = tasks.register<Test>("java22Test") {
    description = "FFM shared-memory tests; requires a JDK >= 22 runtime."
    group = "verification"
    testClassesDirs = java22Test.output.classesDirs
    classpath = java22Test.runtimeClasspath
    useJUnitPlatform()
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED")
}
tasks.named("check") { dependsOn(java22TestTask) }

// Run the (Java 21) `test` task on a chosen runtime JDK to validate the
// baseline degrades correctly: `./gradlew :vgirpc:test -PtestJdk=21`. The FFM
// `java22Test` always runs on the build toolchain (>= 22).
(findProperty("testJdk") as String?)?.let { jdk ->
    tasks.named<Test>("test") {
        javaLauncher.set(javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(jdk.toInt()))
        })
    }
}
