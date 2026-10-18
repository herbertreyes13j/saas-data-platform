
"""Interfaz de línea de comandos del pipeline SAAS."""

import argparse
import sys

from omegaconf import OmegaConf

from saas_pipeline.config import VALID_TENANTS, load_config
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from saas_pipeline.orchestrator import (
    ingest_shared_catalog,
    run_tenant,
)


def parse_args():
    """Define y obtiene los parámetros de ejecución."""

    parser = argparse.ArgumentParser(
        description="SAAS Multi-Tenant Data Pipeline"
    )

    parser.add_argument(
        "--env",
        choices=["dev", "qa", "main"],
        default="dev",
        help="Execution environment",
    )

    parser.add_argument(
        "--tenant",
        choices=["sv", "hn", "ec", "jm", "gt", "pe", "all"],
        default="all",
        help="Tenant to process",
    )

    parser.add_argument(
        "--start-date",
        default=None,
        help="Start date (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--end-date",
        default=None,
        help="End date (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate configuration without running the pipeline",
    )

    return parser.parse_args()


def main():
    """Valida la configuración y prepara la ejecución."""

    args = parse_args()

    try:
        config = load_config(
            env=args.env,
            tenant=args.tenant,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    except (ValueError, OSError) as error:
        print(f"Configuration error: {error}", file=sys.stderr)
        return 2

    if args.validate_only:
        print("Configuration validated successfully")
        print(OmegaConf.to_yaml(config))
        return 0

        # Por ahora, ejecutar un tenant a la vez.


    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("SAAS-Data-Pipeline")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = None

    try:
        spark = configure_spark_with_delta_pip(
            builder
        ).getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        # Cargar el catálogo compartido una sola vez.
        ingest_shared_catalog(spark, config)

        tenants = (
            sorted(VALID_TENANTS)
            if args.tenant == "all"
            else [args.tenant]
        )

        failures = []

        for tenant in tenants:
            print(f"\n=== Processing tenant: {tenant} ===", flush=True)

            try:
                tenant_config = load_config(
                    env=args.env,
                    tenant=tenant,
                    start_date=args.start_date,
                    end_date=args.end_date,
                )

                result = run_tenant(
                    spark,
                    tenant_config,
                    tenant,
                )

                print(f"Pipeline completed: {result}", flush=True)

            except Exception as error:
                print(
                    f"Tenant {tenant} failed: {error}",
                    file=sys.stderr,
                )

                failures.append(tenant)

                if config.execution.fail_fast:
                    print(
                        "Stopping execution: fail_fast is enabled.",
                        file=sys.stderr,
                    )
                    break

        if failures:
            print(f"Failed tenants: {failures}", file=sys.stderr)
            return 1

        return 0

    except Exception as error:
        print(f"Pipeline failed: {error}", file=sys.stderr)
        return 1

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
