plugins {
    application
}

dependencies {
    implementation(project(":vgirpc"))
    implementation(project(":benchmark"))
}

application {
    mainClass.set("farm.query.vgirpc.benchmark.worker.Main")
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
    )
}
