plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":modules:charging-domain"))
    implementation(project(":modules:messaging-contract"))
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-json")
    implementation("org.springframework.boot:spring-boot-starter-integration")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.integration:spring-integration-mqtt")
    implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5")

    testImplementation(project(":modules:test-support"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    workingDir(rootProject.projectDir)
}
