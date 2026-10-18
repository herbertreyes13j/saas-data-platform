
"""Interfaz de línea de comandos del pipeline SAAS."""

import argparse
import sys

from omegaconf import OmegaConf

from saas_pipeline.config import load_config


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

    # La ejecución real se integrará en las siguientes fases.
    print(
        "Pipeline execution is not implemented yet. "
        "Use --validate-only.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
