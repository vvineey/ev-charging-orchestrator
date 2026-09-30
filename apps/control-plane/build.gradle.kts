plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

evaluationDependsOn(":apps:ocpp-gateway")

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    workingDir(rootProject.projectDir)
}

dependencies {
    implementation(project(":modules:charging-domain"))
    implementation(project(":modules:messaging-contract"))
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-json")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("org.flywaydb:flyway-database-postgresql")

    testImplementation(project(":modules:test-support"))
    testImplementation(project(":apps:ocpp-gateway"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("io.zonky.test:embedded-postgres:2.2.2")
    testImplementation(enforcedPlatform("io.zonky.test.postgres:embedded-postgres-binaries-bom:18.3.0"))
}

tasks.test {
    // The crash fixture forks the actual app with the same test runtime dependencies.
    systemProperty("transaction.test.runtime-classpath", sourceSets.test.get().runtimeClasspath.asPath)
}
