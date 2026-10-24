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
    systemProperty("transaction.test.comparison-output",
            layout.buildDirectory.file("experiment-results/exp-0002-candidate-correctness.json").get().asFile.absolutePath)
    systemProperty("transaction.test.performance-output",
            layout.buildDirectory.file("experiment-results/exp-0002-online-performance.json").get().asFile.absolutePath)
    systemProperty("transaction.test.performance-enabled", project.findProperty("performanceEnabled")?.toString() ?: "false")
    systemProperty("transaction.test.window-seconds", project.findProperty("performanceWindowSeconds")?.toString() ?: "300")
    systemProperty("exp0003.cdc.enabled", project.findProperty("exp0003Cdc")?.toString() ?: "false")
    systemProperty("exp0003.cdc.compose-file",
            rootProject.layout.projectDirectory.file("load-tests/outbox/compose.yaml").asFile.absolutePath)
    systemProperty("exp0003.cdc.connector-file",
            rootProject.layout.projectDirectory.file("load-tests/outbox/connector.json").asFile.absolutePath)
    systemProperty("exp0003.burst.enabled", project.findProperty("exp0003Burst")?.toString() ?: "false")
    systemProperty("exp0003.burst.mode", project.findProperty("exp0003BurstMode")?.toString() ?: "POLLING")
    systemProperty("exp0003.burst.batch-size", project.findProperty("exp0003BurstBatchSize")?.toString() ?: "1")
    systemProperty("exp0003.burst.events", project.findProperty("exp0003BurstEvents")?.toString() ?: "1000")
    systemProperty("exp0003.burst.output", project.findProperty("exp0003BurstOutput")?.toString()
            ?: layout.buildDirectory.file("experiment-results/exp0003-burst-result.json").get().asFile.absolutePath)
    systemProperty("exp0003.steady.enabled", project.findProperty("exp0003Steady")?.toString() ?: "false")
    systemProperty("exp0003.steady.mode", project.findProperty("exp0003SteadyMode")?.toString() ?: "POLLING")
    systemProperty("exp0003.steady.rate", project.findProperty("exp0003SteadyRate")?.toString() ?: "200")
    systemProperty("exp0003.steady.seconds", project.findProperty("exp0003SteadySeconds")?.toString() ?: "5")
    systemProperty("exp0003.steady.cdc-poll-ms", project.findProperty("exp0003SteadyCdcPollMs")?.toString() ?: "default")
    systemProperty("exp0003.steady.output", project.findProperty("exp0003SteadyOutput")?.toString()
            ?: layout.buildDirectory.file("experiment-results/exp0003-steady-result.json").get().asFile.absolutePath)
}
