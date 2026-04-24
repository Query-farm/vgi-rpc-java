plugins {
    `java-library`
}

dependencies {
    api(project(":vgirpc"))
    implementation("com.nimbusds:nimbus-jose-jwt:10.0.1")
    // Authenticator.authenticate(HttpServletRequest ...)
    implementation("org.eclipse.jetty.ee10:jetty-ee10-servlet:12.0.16")

    testImplementation(platform("org.junit:junit-bom:5.11.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(testFixtures(project(":vgirpc")))
}
