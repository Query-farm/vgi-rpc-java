plugins {
    `java-library`
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
    implementation("com.github.luben:zstd-jni:1.5.6-8")

    // HTTP transport (server + client)
    implementation("org.eclipse.jetty:jetty-server:$jettyVersion")
    implementation("org.eclipse.jetty:jetty-util:$jettyVersion")
    implementation("org.eclipse.jetty.ee10:jetty-ee10-servlet:$jettyVersion")
    implementation("org.eclipse.jetty:jetty-client:$jettyVersion")

    // Logging backend (only required for tests/worker; library users may bring their own)
    runtimeOnly("org.slf4j:slf4j-simple:$slf4jVersion")

    testImplementation(platform("org.junit:junit-bom:5.11.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}
