from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

import structlog
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from basyx_opcua_bridge.core.exceptions import SecurityError

if TYPE_CHECKING:
    from basyx_opcua_bridge.config.models import SecurityConfig

logger = structlog.get_logger(__name__)

class CertificateManager:
    """Manages X.509 certificates."""

    def __init__(self, config: SecurityConfig) -> None:
        self._config = config
        self._client_cert: Optional[x509.Certificate] = None
        self._client_key: Optional[rsa.RSAPrivateKey] = None

    @property
    def client_cert_path(self) -> Optional[Path]:
        return self._config.client_certificate_path

    @property
    def client_key_path(self) -> Optional[Path]:
        return self._config.client_private_key_path

    async def load_certificates(self) -> None:
        if not self._config.client_certificate_path:
            logger.warning("no_client_certificate_configured")
            return

        try:
            cert_path = Path(self._config.client_certificate_path)
            if not cert_path.exists():
                raise SecurityError(f"Certificate not found: {cert_path}")

            with open(cert_path, "rb") as f:
                self._client_cert = x509.load_pem_x509_certificate(f.read())

            key_path = Path(self._config.client_private_key_path) # type: ignore
            if not key_path.exists():
                raise SecurityError(f"Private key not found: {key_path}")

            with open(key_path, "rb") as f:
                self._client_key = serialization.load_pem_private_key(f.read(), password=None)  # type: ignore[assignment]

            self._validate_certificate()

            logger.info(
                "certificates_loaded",
                subject=self._client_cert.subject.rfc4514_string(),
                expires=self._client_cert.not_valid_after_utc.isoformat(),
            )

        except Exception as e:
            if isinstance(e, SecurityError):
                raise
            raise SecurityError(f"Failed to load certificates: {e}")

    def _validate_certificate(self) -> None:
        if not self._client_cert:
            return
        
        now = datetime.now(timezone.utc)
        if now > self._client_cert.not_valid_after_utc:
            raise SecurityError("Client certificate has expired")

    @staticmethod
    def generate_self_signed(output_dir: Path, common_name: str = "basyx-opcua-bridge", validity_days: int = 365) -> tuple[Path, Path]:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Development"),
        ])
        
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(timezone.utc))
            .not_valid_after(datetime.now(timezone.utc) + timedelta(days=validity_days))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .sign(key, hashes.SHA256())
        )

        cert_path = output_dir / "client.pem"
        key_path = output_dir / "client-key.pem"

        with open(cert_path, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        
        with open(key_path, "wb") as f:
            f.write(key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            ))
            
        return cert_path, key_path
