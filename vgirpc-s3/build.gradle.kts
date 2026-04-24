plugins {
    `java-library`
}

val awsSdkVersion = "2.28.29"

dependencies {
    api(project(":vgirpc"))
    implementation(platform("software.amazon.awssdk:bom:$awsSdkVersion"))
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:s3-transfer-manager")
    implementation("software.amazon.awssdk:regions")
}
