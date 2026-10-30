"""Comandos de operación. Las consultas abren SQLite en solo lectura."""

from __future__ import annotations

import argparse
import os
import signal
import socket
import ssl
import sys
import time
import threading
from pathlib import Path
from urllib.parse import urlsplit

from hikvision_event_bridge.config import ConfigError, load_config, load_dotenv
from hikvision_event_bridge.database import Database, LockBusy, LockLost
from hikvision_event_bridge.logging_config import configure_logging
from hikvision_event_bridge.service import BridgeService
from hikvision_event_bridge.timeutil import parse_configured_time, utc_now


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    try:
        config_path, env_file, rest = _extract_global_options(list(argv))
    except ConfigError as exc:
        print(f"error de configuración: {exc}", file=sys.stderr)
        return 2
    parser = argparse.ArgumentParser(
        prog="hikvision-event-bridge",
        epilog="Opciones globales, antes o después del comando: --config RUTA y --env-file RUTA.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="ejecuta el servicio")
    run_parser.add_argument("--dry-run", action="store_true")
    run_parser.add_argument("--database", help="base distinta; obligatoria con --dry-run")
    run_parser.set_defaults(func=command_run)

    check_parser = sub.add_parser("check-config", help="valida la configuración sin enviar eventos")
    check_parser.set_defaults(func=command_check)

    status_parser = sub.add_parser("status", help="muestra la cola y el avance")
    status_parser.set_defaults(func=command_status)

    retry_parser = sub.add_parser("retry-rejected", help="reactiva entregas rechazadas elegidas")
    retry_parser.add_argument("--id", type=int, dest="delivery_id")
    retry_parser.add_argument("--event-id")
    retry_parser.add_argument("--destination")
    retry_parser.add_argument("--include-conflict", action="store_true")
    retry_parser.set_defaults(func=command_retry)

    backfill_parser = sub.add_parser("backfill", help="recupera una ventana explícita")
    backfill_parser.add_argument("--device", required=True, help="nombre del dispositivo en la configuración")
    backfill_parser.add_argument("--start", required=True)
    backfill_parser.add_argument("--end", required=True)
    backfill_parser.set_defaults(func=command_backfill)

    diag_parser = sub.add_parser("diagnostics", help="comprueba el gateway sin enviar marcaciones")
    diag_parser.add_argument("--subscribe-sample", action="store_true")
    diag_parser.add_argument("--timeout", type=float, default=180, help="segundos totales de la captura")
    diag_parser.add_argument("--max-parts", type=int, default=0, help="tope de seguridad; 0 espera la marcación o el timeout")
    diag_parser.add_argument("--capture", help="archivo nuevo para la muestra; puede contener datos personales")
    diag_parser.set_defaults(func=command_diagnostics)

    compare_parser = sub.add_parser("compare-sample", help="compara una muestra en vivo con el historial, sin guardar ni enviar")
    compare_parser.add_argument("--capture", required=True)
    compare_parser.add_argument("--window-seconds", type=float, default=180)
    compare_parser.add_argument("--device", default="", help="nombre en la configuración; si falta, se usa el devIndex de la muestra")
    compare_parser.set_defaults(func=command_compare_sample)

    args = parser.parse_args(rest)
    args.config = config_path
    args.env_file = env_file
    try:
        load_dotenv(Path(args.env_file))
        return int(args.func(args))
    except ConfigError as exc:
        print(f"error de configuración: {exc}", file=sys.stderr)
        return 2
    except (LockBusy, LockLost) as exc:
        print(str(exc), file=sys.stderr)
        return 3
    except FileNotFoundError as exc:
        print(f"no se encontró la base: {exc.filename or exc}", file=sys.stderr)
        return 4
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2


def _extract_global_options(argv: list[str]) -> tuple[str, str, list[str]]:
    """Acepta --config y --env-file antes o después del subcomando."""
    config_path = "config.toml"
    env_file = ".env"
    rest: list[str] = []
    index = 0
    while index < len(argv):
        token = argv[index]
        if token in {"--config", "--env-file"}:
            if index + 1 >= len(argv):
                raise ConfigError(f"{token} necesita un valor")
            if token == "--config":
                config_path = argv[index + 1]
            else:
                env_file = argv[index + 1]
            index += 2
            continue
        rest.append(token)
        index += 1
    return config_path, env_file, rest


def command_check(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    configure_logging(config.logging.level)
    print("configuración válida")
    print(f"gateway {config.gateway.url} usuario {config.gateway.username}")
    print(f"secreto {config.gateway.password_env}: presente")
    for destination in config.destinations.values():
        print(f"destino {destination.name} {destination.url} secreto {destination.token_env}: presente")
    for device in config.devices.values():
        state = "habilitado" if device.enabled else "deshabilitado"
        print(f"dispositivo {device.name} {state} uid {device.device_uid} destinos {', '.join(device.destinations)}")
    for warning in config.warnings:
        print(f"aviso: {warning}")
    return 0


def command_run(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    original_database = config.storage.sqlite_path
    if args.database:
        config = _with_database(config, Path(args.database), original_database.parent)
    if args.dry_run:
        if not args.database:
            raise ConfigError("dry-run exige --database con una base distinta de la de producción")
        if _same_path(config.storage.sqlite_path, original_database):
            raise ConfigError("dry-run no puede usar la base de producción")
    configure_logging(config.logging.level)
    service = BridgeService(config, dry_run=args.dry_run)
    stop = threading.Event()
    _install_signals(stop)
    service.run(stop)
    return 0


def command_status(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    database = Database(config.storage.sqlite_path, busy_timeout_ms=config.storage.busy_timeout_ms)
    if not config.storage.sqlite_path.exists():
        print("aún no existe la base de datos")
        return 0
    snapshot = database.status()
    deliveries = snapshot["deliveries"]
    print("entregas:")
    if not deliveries:
        print("  ninguna")
    for state, total in sorted(deliveries.items()):
        print(f"  {state}: {total}")
    oldest = snapshot["oldest_open"]
    if oldest:
        age = utc_now() - parse_configured_time(str(oldest))
        print(f"abierta más antigua: {oldest} (hace {int(age.total_seconds())} s)")
    else:
        print("no hay entregas abiertas")
    known = {row["device_uid"]: row for row in snapshot["devices"]}
    print("dispositivos:")
    for device in config.devices.values():
        row = known.get(device.device_uid, {})
        print(
            f"  {device.name} live={row.get('last_live_at') or '-'} "
            f"checkpoint={row.get('checkpoint_until') or '-'} "
            f"backfill={row.get('last_success_at') or '-'} error={row.get('last_error') or '-'}"
        )
    print(f"incidencias: {snapshot['incidents']}")
    counters = snapshot["counters"]
    if counters:
        print("contadores:")
        for name, value in counters.items():
            print(f"  {name}: {value}")
    return 0


def command_retry(args: argparse.Namespace) -> int:
    if args.delivery_id is None and not args.event_id:
        raise ConfigError("indique --id o --event-id")
    config = load_config(Path(args.config))
    database = Database(config.storage.sqlite_path, busy_timeout_ms=config.storage.busy_timeout_ms)
    database.migrate()
    updated = database.retry_rejected(
        delivery_id=args.delivery_id,
        event_id=args.event_id,
        destination_id=args.destination,
        include_conflict=args.include_conflict,
    )
    print(f"entregas reactivadas: {updated}")
    return 0 if updated else 1


def command_backfill(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    device = config.devices.get(args.device)
    if device is None or not device.enabled:
        raise ConfigError(f"dispositivo desconocido o deshabilitado: {args.device}")
    start = parse_configured_time(args.start)
    end = parse_configured_time(args.end)
    if start >= end:
        raise ConfigError("--start debe ser anterior a --end")
    configure_logging(config.logging.level)
    database = Database(config.storage.sqlite_path, busy_timeout_ms=config.storage.busy_timeout_ms)
    database.migrate()
    service = BridgeService(config, database=database)
    service.recover_explicit_window(device, start, end)
    print(f"ventana consultada para {device.name}")
    return 0


def command_diagnostics(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    configure_logging(config.logging.level)
    from hikvision_event_bridge.gateway import GatewayClient, GatewayError

    client = GatewayClient(config)
    try:
        info = client.device_info()
    except GatewayError as exc:
        print(f"gateway: error {exc}")
        return 1
    print(f"gateway: http {info.status} content-type {info.content_type or '-'}")
    if args.subscribe_sample:
        if args.timeout <= 0:
            raise ConfigError("--timeout debe ser mayor que 0")
        if args.max_parts < 0:
            raise ConfigError("--max-parts no puede ser negativo")
        return _capture_sample(args, config, client)
    for destination in config.destinations.values():
        try:
            probe = probe_origin(destination.url, config.timeouts.connect_seconds, verify=True)
        except OSError as exc:
            print(f"destino {destination.name}: no alcanzable ({exc.__class__.__name__})")
            continue
        print(f"destino {destination.name}: {probe}")
    print("no se abrió la suscripción. Use --subscribe-sample para esperar una marcación.")
    return 0


def command_compare_sample(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    configure_logging(config.logging.level)
    from hikvision_event_bridge.diagnostics import compare_capture
    from hikvision_event_bridge.gateway import GatewayClient, GatewayError

    if args.window_seconds <= 0:
        raise ConfigError("--window-seconds debe ser mayor que 0")
    client = GatewayClient(config)
    try:
        report = compare_capture(
            Path(args.capture),
            config,
            client,
            window_seconds=args.window_seconds,
            device_name=args.device,
        )
    except GatewayError as exc:
        print(f"gateway: error {exc}")
        return 1
    except ConfigError as exc:
        raise
    except ValueError as exc:
        print(f"muestra: {exc}")
        return 1
    print(report.text)
    return 0 if report.searched else 1


def _capture_sample(args: argparse.Namespace, config, client) -> int:
    from hikvision_event_bridge.diagnostics import access_device_uid
    from hikvision_event_bridge.gateway import subscription_id_from_payload
    from hikvision_event_bridge.normalization import loads_gateway_json

    allowed = {device.device_uid for device in config.devices.values() if device.enabled}
    print("aviso: la muestra puede contener nombres, tarjetas u otros datos personales", file=sys.stderr)
    print(
        f"esperando un AccessControllerEvent de un dispositivo configurado, hasta {args.timeout:.0f} s. "
        "Haz una sola marcación en el reloj. No se envía nada a Rancherito."
    )
    content_type, parts = client.subscribe()
    subscription_id = None
    captured = 0
    found = False
    handle = None
    deadline = time.monotonic() + args.timeout
    try:
        if args.capture:
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            handle = os.open(args.capture, flags, 0o600)
        for part in parts:
            if time.monotonic() > deadline:
                print("tiempo agotado antes de una marcación del dispositivo configurado")
                break
            payload = None
            if "json" in part.content_type.lower() or not part.content_type:
                try:
                    payload = loads_gateway_json(part.body)
                except ValueError:
                    payload = None
                found_id = subscription_id_from_payload(payload)
                if found_id:
                    subscription_id = found_id
            if handle is not None:
                os.write(handle, b"-- part\n")
                os.write(handle, part.body)
                os.write(handle, b"\n")
            captured += 1
            print(f"parte {captured} tipo {part.content_type or '-'} bytes {len(part.body)} demasiado_grande {part.too_large}")
            device_uid = access_device_uid(payload, allowed)
            if device_uid:
                print(f"marcación encontrada en {device_uid}")
                found = True
                break
            if payload is not None and access_device_uid(payload, None):
                print("evento de otro dispositivo; se sigue esperando")
            if args.max_parts and captured >= args.max_parts:
                print("se alcanzó --max-parts sin la marcación buscada")
                break
    finally:
        if handle is not None:
            os.close(handle)
            try:
                os.chmod(args.capture, 0o600)
            except OSError:
                pass
        client.close_stream()
        if subscription_id:
            client.unsubscribe(subscription_id)
            print(f"suscripción {subscription_id} cancelada")
    print(f"muestra de {captured} partes; content-type {content_type}")
    return 0 if found else 1


def probe_origin(url: str, timeout: float, *, verify: bool) -> str:
    parts = urlsplit(url)
    if parts.hostname is None:
        raise OSError("url sin host")
    port = parts.port or (443 if parts.scheme == "https" else 80)
    connection = socket.create_connection((parts.hostname, port), timeout=timeout)
    try:
        if parts.scheme != "https":
            return "tcp_ok"
        context = ssl.create_default_context() if verify else ssl._create_unverified_context()
        with context.wrap_socket(connection, server_hostname=parts.hostname) as tls:
            return f"tls_ok {tls.version()}"
    finally:
        connection.close()


def _with_database(config, database: Path, base: Path):
    from dataclasses import replace

    path = database if database.is_absolute() else (base / database).resolve()
    return replace(config, storage=replace(config.storage, sqlite_path=path))


def _same_path(left: Path, right: Path) -> bool:
    if left.resolve() == right.resolve():
        return True
    try:
        return left.exists() and right.exists() and os.path.samefile(left, right)
    except OSError:
        return False


def _install_signals(stop) -> None:
    def handle(_signum, _frame) -> None:
        stop.set()

    signal.signal(signal.SIGINT, handle)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle)
