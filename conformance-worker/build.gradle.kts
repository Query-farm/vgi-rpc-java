plugins {
    application
}

dependencies {
    implementation(project(":vgirpc"))
    implementation(project(":conformance"))
}

application {
    mainClass.set("farm.query.vgirpc.conformance.worker.Main")
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
    )
}
