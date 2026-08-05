plugins {
    application
}

dependencies {
    implementation(project(":vgirpc"))
    implementation(project(":conformance"))
    // Optional — only needed by --auth-jwt / --auth-pkce modes
    implementation(project(":vgirpc-oauth"))
    // --sticky-auth declares an Authenticator lambda inline, so this module has to
    // name HttpServletRequest at compile time. vgirpc keeps Jetty on `implementation`
    // (not exposed to consumers), but it is on the runtime classpath transitively —
    // hence compileOnly rather than implementation.
    compileOnly("jakarta.servlet:jakarta.servlet-api:6.0.0")
    // SLF4J backend for this runnable worker (vgirpc no longer ships one).
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

application {
    mainClass.set("farm.query.vgirpc.conformance.worker.Main")
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
    )
}
