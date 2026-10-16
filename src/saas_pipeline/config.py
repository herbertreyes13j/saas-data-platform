
from datetime import date
from pathlib import Path

from omegaconf import OmegaConf


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_ROOT = PROJECT_ROOT / "config"

VALID_ENVS = {"dev", "qa", "main"}
VALID_TENANTS = {"sv", "hn", "ec", "jm", "gt", "pe"}


def load_config(
    env="dev",
    tenant="all",
    start_date=None,
    end_date=None,
):
    """Carga y valida la configuración jerárquica."""

    env = env.lower()
    tenant = tenant.lower()

    if env not in VALID_ENVS:
        raise ValueError(f"Invalid environment: {env}")

    if tenant != "all" and tenant not in VALID_TENANTS:
        raise ValueError(f"Invalid tenant: {tenant}")

    base_config = OmegaConf.load(
        CONFIG_ROOT / "base.yaml"
    )

    env_config = OmegaConf.load(
        CONFIG_ROOT / "env" / f"{env}.yaml"
    )

    config = OmegaConf.merge(base_config, env_config)

    if tenant != "all":
        tenant_config = OmegaConf.load(
            CONFIG_ROOT / "tenants" / f"{tenant}.yaml"
        )
        config = OmegaConf.merge(config, tenant_config)

    config.execution.tenant = tenant

    if start_date is not None:
        config.execution.start_date = start_date

    if end_date is not None:
        config.execution.end_date = end_date

    start = date.fromisoformat(config.execution.start_date)
    end = date.fromisoformat(config.execution.end_date)

    if start > end:
        raise ValueError(
            "start_date cannot be greater than end_date"
        )

    return config
