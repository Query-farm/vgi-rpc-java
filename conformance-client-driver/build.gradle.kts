plugins {
    application
}

dependencies {
    implementation(project(":vgirpc"))
    // The control channel is newline-delimited JSON. Jackson is already on the
    // library's own classpath; naming it here keeps this module's use of it
    // explicit rather than inherited by accident.
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    // SLF4J backend, pinned to stderr below. vgirpc ships none, and the default
    // NOP binder would swallow a real diagnostic.
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

application {
    mainClass.set("farm.query.vgirpc.conformance.driver.Main")
    // stdout is the control channel and nothing else: a single stray line
    // desynchronises the whole run, so every logger is pinned to stderr and
    // quietened. `slf4j.simpleLogger.logFile=System.err` is the default, set
    // explicitly because it is load-bearing here rather than cosmetic.
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-Dorg.slf4j.simpleLogger.logFile=System.err",
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
    )
}
