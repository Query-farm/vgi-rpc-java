plugins {
    java
    // Applied (below) only to the published library modules. Targets the
    // Sonatype Central Portal by default and handles signing + sources/javadoc.
    id("com.vanniktech.maven.publish") version "0.36.0" apply false
}

allprojects {
    group = "farm.query"
    version = "0.17.0"

    repositories {
        mavenCentral()
    }
}

// Library modules published to Maven Central. The conformance/benchmark
// service definitions and their runnable workers are internal test/bench
// harnesses and are intentionally NOT published.
val publishedModules = setOf("vgirpc", "vgirpc-oauth", "vgirpc-s3", "vgirpc-gcs")

// One-line POM descriptions per published artifact.
val moduleDescriptions = mapOf(
    "vgirpc" to "Transport-agnostic RPC framework built on Apache Arrow IPC.",
    "vgirpc-oauth" to "Optional OAuth/JWT (JWKS, PKCE, signed cookies) support for vgi-rpc.",
    "vgirpc-s3" to "Amazon S3 ExternalStorage backend for vgi-rpc.",
    "vgirpc-gcs" to "Google Cloud Storage ExternalStorage backend for vgi-rpc.",
)

subprojects {
    apply(plugin = "java")
    // Coverage. Applied everywhere so the JUnit lane is instrumented for free
    // (`./gradlew test jacocoTestReport`). The high-value lane — the
    // conformance worker subprocess — is wired separately in vgirpc's build
    // via a JacocoReport task fed by per-process .exec files (see run_tests.sh
    // --coverage). Pin the tool so both lanes agree on one JaCoCo version.
    apply(plugin = "jacoco")
    extensions.configure<JacocoPluginExtension> {
        toolVersion = "0.8.13"
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            // JDK 25 toolchain so the vgirpc multi-release JAR can build its
            // java.lang.foreign (shared-memory) overlay. Per-module release
            // levels below control the actual bytecode target.
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    // Everything targets a Java 21 baseline (vgirpc overrides individual tasks
    // for its 21/22 multi-release split in its own build file). Published
    // modules so consumers aren't forced onto a newer JDK; the worker/bench
    // modules so the runnable workers also launch on a JDK 21 runtime (where
    // the FFM shared-memory overlay is absent and the pipe fallback is used).
    // The toolchain is still JDK 25 — only the bytecode target is 21.
    tasks.withType<JavaCompile>().configureEach {
        options.release.set(21)
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

    if (name in publishedModules) {
        apply(plugin = "com.vanniktech.maven.publish")

        // Catch real doc problems (broken HTML, dangling @link, malformed tags)
        // but don't demand @param/@return boilerplate on every accessor —
        // the codebase predates strict doc-comment hygiene.
        tasks.withType<Javadoc>().configureEach {
            val logo = rootProject.layout.projectDirectory.file("assets/vgi-logo.png").asFile
            val brandCss = rootProject.layout.projectDirectory.file("assets/javadoc/query-farm.css").asFile
            val overviewHtml = file("src/main/javadoc/overview.html")
            inputs.file(logo)
            inputs.file(brandCss)
            with(options as StandardJavadocDocletOptions) {
                addStringOption("Xdoclint:all,-missing", "-quiet")
                windowTitle = "${project.name} $version — Query.Farm Arrow RPC for Java"
                docTitle = "<img src='vgi-logo.png' alt='Vector Gateway Interface' class='qf-logo'>" +
                    "${project.name} $version API"
                header = "<a href='https://query.farm' target='_top'>🚜 Query.Farm</a>"
                bottom = "Copyright © 2026 <a href='https://query.farm'>Query Farm LLC</a> · " +
                    "<a href='https://github.com/Query-farm/vgi-rpc-java'>github.com/Query-farm/vgi-rpc-java</a> · " +
                    "Apache License 2.0"
                // Modules without an overview page (the storage/auth add-ons)
                // just get the default summary table.
                if (overviewHtml.isFile) {
                    inputs.file(overviewHtml)
                    overview = overviewHtml.path
                }
                addStringOption("-add-stylesheet", brandCss.path)
            }
            // The doctitle's logo isn't a doclet-managed resource; drop it into
            // the output root so the javadoc jar stays self-contained.
            doLast {
                logo.copyTo(destinationDir!!.resolve("vgi-logo.png"), overwrite = true)
            }
        }

        extensions.configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
            // Upload to the Central Portal and auto-release once validation
            // passes (no manual "publish" click in the Portal UI). Flip to
            // publishToMavenCentral(false) if you'd rather gate releases by hand.
            publishToMavenCentral(automaticRelease = true)

            // Sign only when a key is available (CI / local release). Plain
            // `publishToMavenLocal` and contributor builds without a key still work.
            if (project.findProperty("signingInMemoryKey") != null) {
                signAllPublications()
            }

            coordinates(group.toString(), name, version.toString())

            pom {
                name.set(this@subprojects.name)
                description.set(
                    moduleDescriptions[this@subprojects.name]
                        ?: "vgi-rpc-java module ${this@subprojects.name}."
                )
                url.set("https://github.com/Query-farm/vgi-rpc-java")
                inceptionYear.set("2026")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                organization {
                    name.set("Query Farm LLC")
                    url.set("https://query.farm")
                }
                developers {
                    developer {
                        name.set("Query Farm LLC")
                        url.set("https://query.farm")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/Query-farm/vgi-rpc-java.git")
                    developerConnection.set("scm:git:git@github.com:Query-farm/vgi-rpc-java.git")
                    url.set("https://github.com/Query-farm/vgi-rpc-java")
                }
            }
        }
    }
}
