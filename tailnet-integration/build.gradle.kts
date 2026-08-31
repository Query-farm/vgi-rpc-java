plugins {
    application
}

dependencies {
    implementation(project(":vgirpc"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")

    testImplementation(platform("org.junit:junit-bom:5.11.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("farm.query.vgirpc.tailnet.Main")
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
    )
}
