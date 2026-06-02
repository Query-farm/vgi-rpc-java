plugins {
    application
}

dependencies {
    implementation(project(":vgirpc"))
    implementation(project(":benchmark"))
    // SLF4J backend for this runnable worker (vgirpc no longer ships one).
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

application {
    mainClass.set("farm.query.vgirpc.benchmark.worker.Main")
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
    )
}
