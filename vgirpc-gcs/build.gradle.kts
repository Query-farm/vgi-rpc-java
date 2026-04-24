plugins {
    `java-library`
}

dependencies {
    api(project(":vgirpc"))
    implementation(platform("com.google.cloud:libraries-bom:26.52.0"))
    implementation("com.google.cloud:google-cloud-storage")
}
