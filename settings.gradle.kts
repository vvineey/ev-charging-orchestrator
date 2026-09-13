rootProject.name = "ev-charging-orchestrator"

include(
    ":modules:charging-domain",
    ":modules:messaging-contract",
    ":modules:test-support",
    ":apps:control-plane",
    ":apps:ocpp-gateway",
    ":apps:telemetry-worker",
    ":apps:ai-worker",
    ":apps:charger-simulator",
)
