plugins {
    java
}

allprojects {
    group = "farm.query"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "maven-publish")

    afterEvaluate {
        // Only publish modules that produce a Java component (skip
        // platform-only or test-only projects that don't define one).
        if (components.findByName("java") != null) {
            extensions.configure<PublishingExtension> {
                publications {
                    create<MavenPublication>("maven") {
                        from(components["java"])
                    }
                }
            }
        }
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.release.set(25)
        options.compilerArgs.addAll(
            listOf(
                "-Xlint:all,-serial,-processing",
                "-parameters",
            )
        )
        options.encoding = "UTF-8"
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        // Arrow's memory module needs access to java.nio internals;
        // FFM (shm_open/mmap) needs native access without warnings.
        jvmArgs(
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--enable-native-access=ALL-UNNAMED",
        )
    }
}
