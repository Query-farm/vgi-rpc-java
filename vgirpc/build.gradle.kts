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

    // Logging backend. This is a *library* — consumers bring their own SLF4J
    // binding, so slf4j-simple must NOT leak into the published runtime POM.
    // Tests use it directly; the runnable worker modules declare it themselves.
    testRuntimeOnly("org.slf4j:slf4j-simple:$slf4jVersion")

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

// The java-test-fixtures variants (HttpRequestStub etc.) are test-only helpers
// and must not be published to Maven Central — strip them from the java
// component so they don't appear in the POM / Gradle module metadata. Done in
// afterEvaluate because the maven-publish plugin adds the *sources* test-fixtures
// variant late; this block must run after that to catch all three.
afterEvaluate {
    (components["java"] as AdhocComponentWithVariants).let { javaComponent ->
        listOf("testFixturesApiElements", "testFixturesRuntimeElements", "testFixturesSourcesElements").forEach { cfg ->
            configurations.findByName(cfg)?.let { javaComponent.withVariantsFromConfiguration(it) { skip() } }
        }
    }
}

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

// --- Coverage: the conformance-worker subprocess lane ---
//
// The library's real exercise is the Python conformance suite, which spawns the
// worker as a separate JVM over every transport. To measure what that suite
// covers we attach the JaCoCo agent to each worker process (see run_tests.sh
// --coverage) and merge the resulting per-process .exec files into one report
// against this module's bytecode (the 21 baseline + the java22 FFM overlay,
// since the worker runs on JDK 25 and loads FfmShm).
val jacocoAgentRuntime by configurations.creating
dependencies {
    // The `runtime` classifier IS the standalone agent jar usable with
    // -javaagent (vs. the default artifact, which embeds it as a resource).
    jacocoAgentRuntime("org.jacoco:org.jacoco.agent:0.8.13:runtime")
}

// Stage the agent jar at a stable path so the worker wrapper can reference it.
tasks.register<Copy>("extractJacocoAgent") {
    description = "Stage the JaCoCo runtime agent jar for the conformance worker subprocess."
    group = "verification"
    from(jacocoAgentRuntime)
    into(layout.buildDirectory.dir("jacoco-agent"))
    rename { "jacocoagent.jar" }
}

// Directory the worker wrapper writes per-process .exec files into. Overridable
// so CI can point it at a shared location across sharded transport jobs.
val conformanceExecDir = (findProperty("conformanceExecDir") as String?)
    ?.let { file(it) }
    ?: layout.buildDirectory.dir("jacoco-conformance").get().asFile

tasks.register<org.gradle.testing.jacoco.tasks.JacocoReport>("jacocoConformanceReport") {
    description = "Coverage report for the Python conformance suite (worker subprocess lane)."
    group = "verification"
    // The worker runs on JDK >= 25, so the multi-release JAR loads the java22
    // overlay (FfmShm + the real ShmFactory) from META-INF/versions/22 — never
    // the baseline stub. Report against main's classes EXCEPT the shadowed
    // ShmFactory, plus the java22 overlay. (Feeding both source sets whole would
    // hand JaCoCo two classes with the VM name ShmFactory and fail the report.)
    classDirectories.setFrom(
        sourceSets["main"].output.classesDirs.asFileTree.matching {
            exclude("**/shm/ShmFactory.class", "**/shm/ShmFactory\$*.class")
        },
        java22.output.classesDirs,
    )
    sourceDirectories.setFrom(sourceSets["main"].java.srcDirs, java22.java.srcDirs)
    executionData(fileTree(conformanceExecDir) { include("*.exec") })
    reports {
        html.required.set(true)
        xml.required.set(true)
        csv.required.set(false)
    }
    doFirst {
        val execs = fileTree(conformanceExecDir) { include("*.exec") }.files
        if (execs.isEmpty()) {
            throw GradleException(
                "No .exec files in $conformanceExecDir — run `./run_tests.sh --coverage` first."
            )
        }
        logger.lifecycle("Merging ${execs.size} worker .exec file(s) from $conformanceExecDir")
    }
}

// Overall library coverage = JUnit lane (test + the FFM java22Test) merged with
// the conformance subprocess lane. This is the honest "is coverage adequate"
// number: the conformance suite deliberately skips auth/JWT (covered by JUnit
// and the dedicated test_java_auth_*.py suites), so neither lane alone tells the
// whole story. Run `./gradlew :vgirpc:test :vgirpc:java22Test` and
// `./run_tests.sh --coverage` first to populate every .exec source.
tasks.register<org.gradle.testing.jacoco.tasks.JacocoReport>("jacocoMergedReport") {
    description = "Combined coverage: JUnit lane + conformance subprocess lane."
    group = "verification"
    classDirectories.setFrom(
        sourceSets["main"].output.classesDirs.asFileTree.matching {
            exclude("**/shm/ShmFactory.class", "**/shm/ShmFactory\$*.class")
        },
        java22.output.classesDirs,
    )
    sourceDirectories.setFrom(sourceSets["main"].java.srcDirs, java22.java.srcDirs)
    executionData(
        fileTree(layout.buildDirectory.dir("jacoco")) { include("*.exec") },
        fileTree(conformanceExecDir) { include("*.exec") },
    )
    reports {
        html.required.set(true)
        xml.required.set(true)
        csv.required.set(false)
    }
}
