plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":modules:charging-domain"))
    implementation(project(":modules:messaging-contract"))
    implementation("org.springframework.boot:spring-boot-starter")

    testImplementation(project(":modules:test-support"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
