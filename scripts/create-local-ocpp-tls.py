#!/usr/bin/env python3
"""Create ignored, localhost-only OCPP WSS material and one charger credential."""
import os
from pathlib import Path
import secrets
import subprocess


def main():
    root = Path(__file__).resolve().parent.parent
    env_file = root / ".env"
    if not env_file.is_file():
        raise SystemExit("Create .env from .env.example first.")
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        raise SystemExit("Set JAVA_HOME to your Java 21 installation.")
    original = env_file.read_text()
    for line in original.splitlines():
        name, _, value = line.partition("=")
        if name in {"OCPP_WSS_KEY_STORE_PASSWORD", "OCPP_STATION_PASSWORD"} and value.strip():
            raise SystemExit("An OCPP secret is already set; existing .env was preserved.")
    keytool = str(Path(java_home) / "bin" / "keytool")
    directory = root / "secrets"
    key_store = directory / "ocpp-gateway.p12"
    certificate = directory / "ocpp-gateway.crt"
    if key_store.exists() or certificate.exists():
        raise SystemExit("OCPP TLS material already exists; existing keys and .env were preserved.")

    os.umask(0o077)
    directory.mkdir(mode=0o700, exist_ok=True)
    store_password = secrets.token_hex(24)
    charger_password = secrets.token_hex(24)
    process_env = dict(os.environ, EVC_OCPP_TLS_PASSWORD=store_password)

    def run(*arguments):
        subprocess.run([keytool, *arguments], env=process_env, check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    try:
        run("-genkeypair", "-alias", "ocpp-gateway", "-keyalg", "RSA", "-keysize", "2048",
            "-sigalg", "SHA256withRSA", "-dname", "CN=localhost",
            "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-validity", "30",
            "-storetype", "PKCS12", "-keystore", str(key_store),
            "-storepass:env", "EVC_OCPP_TLS_PASSWORD", "-keypass:env", "EVC_OCPP_TLS_PASSWORD")
        run("-exportcert", "-rfc", "-alias", "ocpp-gateway", "-keystore", str(key_store),
            "-storepass:env", "EVC_OCPP_TLS_PASSWORD", "-file", str(certificate))
    except subprocess.CalledProcessError:
        key_store.unlink(missing_ok=True)
        certificate.unlink(missing_ok=True)
        raise SystemExit("Local OCPP TLS generation failed; .env was preserved.") from None

    settings = {
        "OCPP_WSS_KEY_STORE": "file:./secrets/ocpp-gateway.p12",
        "OCPP_WSS_KEY_STORE_PASSWORD": store_password,
        "OCPP_STATION_PASSWORD": charger_password,
    }
    lines = []
    for line in original.splitlines(keepends=True):
        name = line.split("=", 1)[0]
        if name not in settings:
            lines.append(line)
    updated = "".join(lines).rstrip("\n") + "\n\n# Local OCPP WSS and charger credential\n"
    updated += "".join(f"{name}={value}\n" for name, value in settings.items())
    temporary = root / ".env.ocpp-tls-tmp"
    temporary.write_text(updated)
    temporary.chmod(0o600)
    temporary.replace(env_file)
    print("Local OCPP TLS and charger credential created in ignored paths; .env updated (0600).")


if __name__ == "__main__":
    main()
