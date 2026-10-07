plugins {
    `java-library`
}

dependencies {
    api(project(":vgirpc"))

    // The reflection client tests drive this module's services on every transport,
    // against an in-process Java server and the Python reference conformance server.
    testImplementation(platform("org.junit:junit-bom:5.11.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}
