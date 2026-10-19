"""Pruebas de ejecución multi-tenant y fail_fast."""

from argparse import Namespace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from omegaconf import OmegaConf

from saas_pipeline import cli


@pytest.mark.parametrize("fail_fast", [True, False])
def test_multi_tenant_fail_fast(monkeypatch, fail_fast):
    """Verifica si el pipeline continúa o se detiene ante errores."""

    args = Namespace(
        env="dev",
        tenant="all",
        start_date="2025-01-01",
        end_date="2025-06-30",
        validate_only=False,
    )

    monkeypatch.setattr(cli, "parse_args", lambda: args)

    config = OmegaConf.create({
        "execution": {"fail_fast": fail_fast}
    })

    monkeypatch.setattr(
        cli,
        "load_config",
        lambda **kwargs: config,
    )

    # Simular Spark para evitar ejecuciones reales.
    spark = MagicMock()
    builder = MagicMock()

    builder.master.return_value = builder
    builder.appName.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.return_value = spark

    monkeypatch.setattr(
        cli,
        "SparkSession",
        SimpleNamespace(builder=builder),
    )

    monkeypatch.setattr(
        cli,
        "configure_spark_with_delta_pip",
        lambda spark_builder: spark_builder,
    )

    shared_catalog = MagicMock()

    monkeypatch.setattr(
        cli,
        "ingest_shared_catalog",
        shared_catalog,
    )

    processed = []

    def fake_run_tenant(spark, config, tenant):
        processed.append(tenant)

        if tenant == "ec":
            raise RuntimeError("Simulated Ecuador failure")

        return {"tenant": tenant}

    monkeypatch.setattr(
        cli,
        "run_tenant",
        fake_run_tenant,
    )

    result = cli.main()

    # Como Ecuador falló, la ejecución no debe reportar éxito.
    assert result == 1

    if fail_fast:
        assert processed == ["ec"]
    else:
        assert processed == sorted(cli.VALID_TENANTS)

    # El catálogo se carga una sola vez.
    shared_catalog.assert_called_once()

    # Spark debe cerrarse incluso si hay errores.
    spark.stop.assert_called_once()