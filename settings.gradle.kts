rootProject.name = "ev-charging-orchestrator"

include(
    ":modules:charging-domain",
    ":modules:messaging-contract",
    ":modules:test-support",
    ":apps:control-plane",
    ":apps:ocpp-gateway",
    ":apps:mqtt-adapter",
    ":apps:telemetry-worker",
    ":apps:ai-worker",
    ":apps:charger-simulator",
)
