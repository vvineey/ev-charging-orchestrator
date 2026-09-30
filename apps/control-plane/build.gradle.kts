plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    workingDir(rootProject.projectDir)
}

dependencies {
    implementation(project(":modules:charging-domain"))
    implementation(project(":modules:messaging-contract"))
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-json")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")

    testImplementation(project(":modules:test-support"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
