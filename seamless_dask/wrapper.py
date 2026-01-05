"""CLI wrapper that launches and supervises a Dask cluster."""

from __future__ import annotations

import logging
import argparse
import atexit
import json
import os
import random
import shlex
import signal
import socket
import sys
import time
from dataclasses import dataclass
from datetime import timedelta
from importlib import import_module
from types import FunctionType
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    TextIO,
    Tuple,
)


STATUS_FILE_WAIT_TIMEOUT = 20.0
INACTIVITY_CHECK_INTERVAL = 1.0
WORKER_RECOVERY_INTERVAL = 30.0
WORKER_STUCK_TIMEOUT = 300.0
ADAPTIVE_WAIT_COUNT = 20
ADAPTIVE_INTERVAL = "30s"
DEFAULT_TMPDIR = "/tmp"
DEFAULT_TRANSFORMATION_THROTTLE = 3
DEFAULT_UNKNOWN_TASK_DURATION = "1m"
DEFAULT_TARGET_DURATION = "10m"
DEFAULT_LIFETIME_STAGGER = "4m"
DEFAULT_LIFETIME_GRACE = "1m"
JOBQUEUE_SYSTEMS = (
    "slurm",
    "oar",
    "pbs",
    "sge",
    "lsf",
    "htcondor",
    "moab",
    "condor",
)

status_tracker: "StatusFileTracker | None" = None
log_handle: Optional[TextIO] = None

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)


def configure_log_handle(status_contents: Mapping[str, Any]) -> None:
    global log_handle
    log_path = status_contents.get("log")
    if not isinstance(log_path, str) or not log_path:
        return
    try:
        log_handle = open(log_path, "a", encoding="utf-8")
    except Exception:
        log_handle = None


def _close_log_handle() -> None:
    global log_handle
    if log_handle is None:
        return
    try:
        log_handle.close()
    except Exception:
        pass
    log_handle = None


atexit.register(_close_log_handle)


def log_print(*args: Any, **kwargs: Any) -> None:
    if log_handle is not None:
        kwargs["file"] = log_handle
    kwargs.setdefault("flush", True)
    print(*args, **kwargs)


class StatusFileTracker:
    def __init__(self, path: str, base_contents: dict):
        self.path = path
        self._base_contents = dict(base_contents)
        self.running_written = False

    def _write(self, payload: dict):
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as status_stream:
            json.dump(payload, status_stream, indent=4)
            status_stream.write("\n")
        os.replace(tmp_path, self.path)

    def write_running(self, scheduler_port: int, dashboard_port: int):
        payload = dict(self._base_contents)
        payload["port"] = scheduler_port
        payload["dashboard_port"] = dashboard_port
        payload["status"] = "running"
        self._write(payload)
        self._base_contents = payload
        self.running_written = True

    def write_failed(self):
        payload = dict(self._base_contents)
        payload["status"] = "failed"
        self._write(payload)


def raise_startup_error(exc: BaseException):
    if status_tracker and not status_tracker.running_written:
        status_tracker.write_failed()
    raise exc


def wait_for_status_file(path: str, timeout: float = STATUS_FILE_WAIT_TIMEOUT):
    deadline = time.monotonic() + timeout
    while True:
        try:
            with open(path, "r", encoding="utf-8") as status_stream:
                contents = json.load(status_stream)
                break
        except FileNotFoundError:
            if time.monotonic() >= deadline:
                print(
                    f"Status file '{path}' not found after {int(timeout)} seconds",
                    file=sys.stderr,
                )
                sys.exit(1)
            time.sleep(0.1)
            continue
        except json.JSONDecodeError as exc:
            print(
                f"Status file '{path}' is not valid JSON: {exc}",
                file=sys.stderr,
            )
            sys.exit(1)

    if not isinstance(contents, dict):
        print(
            f"Status file '{path}' must contain a JSON object",
            file=sys.stderr,
        )
        sys.exit(1)

    return contents


def pick_random_free_port(
    host: str, start: int, end: int, *, exclude: Iterable[int] = ()
):
    if start < 0 or end > 65535:
        raise RuntimeError("--port-range values must be between 0 and 65535")
    if start > end:
        raise RuntimeError("--port-range START must be less than or equal to END")

    excluded = set(exclude)
    span = end - start + 1
    attempted = set()
    while len(attempted) < span:
        port = random.randint(start, end)
        if port in attempted or port in excluded:
            continue
        attempted.add(port)
        try:
            with socket.create_server((host, port), reuse_port=False):
                pass
        except OSError:
            continue
        return port

    raise RuntimeError(f"No free port available in range {start}-{end}")


def format_bash_export(var: str, value: Any) -> str:
    if isinstance(value, dict) or isinstance(value, list):
        value = json.dumps(value)
    if value is None:
        value = ""
    return f"export {var}={shlex.quote(str(value))}"


def dask_key_to_env_var(key: str) -> str:
    parts = key.replace("-", "_").split(".")
    encoded = "__".join(part.upper() for part in parts if part)
    return f"DASK_{encoded}"


def parse_timedelta_value(value: Any) -> timedelta:
    """Parse a time interval using Dask utilities (no distributed/jobqueue import)."""

    import dask.utils

    # Allow plain HH:MM:SS (or MM:SS) strings, which dask.utils.parse_timedelta
    # may reject.
    if isinstance(value, str) and ":" in value:
        parts = value.split(":")
        if all(p.isdigit() for p in parts):
            try:
                if len(parts) == 3:
                    h, m, s = (int(p) for p in parts)
                    return timedelta(hours=h, minutes=m, seconds=s)
                if len(parts) == 2:
                    m, s = (int(p) for p in parts)
                    return timedelta(minutes=m, seconds=s)
            except Exception:
                pass

    parsed = dask.utils.parse_timedelta(value)
    if isinstance(parsed, timedelta):
        return parsed
    # dask.utils.parse_timedelta may return a numeric value (seconds)
    if isinstance(parsed, (int, float)):
        return timedelta(seconds=float(parsed))
    # Fallback: try to coerce stringified numeric seconds
    try:
        return timedelta(seconds=float(parsed))
    except Exception as exc:  # pragma: no cover - defensive
        raise TypeError(f"Cannot interpret timedelta from {parsed!r}") from exc


def normalize_port_range(value: Any) -> Tuple[int, int]:
    if isinstance(value, (list, tuple)) and len(value) == 2:
        start, end = value
        return int(start), int(end)
    if isinstance(value, str) and ":" in value:
        start, end = value.split(":", 1)
        return int(start), int(end)
    raise ValueError(f"Invalid port range: {value}")


def render_port_range_string(port_range: Tuple[int, int]) -> str:
    start, end = port_range
    return f"{start}:{end}"


def ensure_list(value: Any, name: str) -> List[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise RuntimeError(f"Parameter '{name}' must be a list if defined")
    return value


def parse_bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("1", "true", "yes", "y", "on"):
            return True
        if lowered in ("0", "false", "no", "n", "off"):
            return False
    raise RuntimeError(f"Parameter '{name}' must be a boolean")


def merge_flat_config(flat_config: Mapping[str, Any]) -> Dict[str, Any]:
    root: Dict[str, Any] = {}
    for dotted_key, value in flat_config.items():
        parts = dotted_key.split(".")
        current: MutableMapping[str, Any] = root
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            elif not isinstance(current[part], dict):
                raise RuntimeError(f"Conflicting config path at '{dotted_key}'")
            current = current[part]
        current[parts[-1]] = value
    return root


@dataclass
class WrapperConfig:
    common: Dict[str, Dict[str, Any]]
    cores: int
    worker_threads: Optional[int]
    worker_processes: int
    worker_port_range: str
    nanny_port_range: str
    worker_lifetime: Any
    worker_lifetime_stagger: Any
    worker_resources: Optional[Dict[str, Any]]
    jobqueue_config: Dict[str, Dict[str, Any]]
    dask_config: Dict[str, Any]
    env_exports: List[str]
    scheduler_port: int
    dashboard_port: int
    interactive: bool
    maximum_jobs: Optional[int]
    memory_per_core_property_name: Optional[str]
    pure_dask: bool


def build_wrapper_configuration(
    *,
    host: str,
    port_range: Tuple[int, int],
    parameters: Mapping[str, Any],
) -> WrapperConfig:
    if not parameters:
        raise RuntimeError("Status file must define a 'parameters' object")
    if not isinstance(parameters, Mapping):
        raise RuntimeError("Status file 'parameters' must be a JSON object")

    walltime = parameters.get("walltime")
    if walltime is None:
        raise RuntimeError("Missing required parameter 'walltime'")
    cores = parameters.get("cores")
    if cores is None:
        raise RuntimeError("Missing required parameter 'cores'")
    memory = parameters.get("memory")
    if memory is None:
        raise RuntimeError("Missing required parameter 'memory'")

    try:
        cores = int(cores)
    except Exception:
        raise RuntimeError("Parameter 'cores' must be an integer")
    if cores <= 0:
        raise RuntimeError("Parameter 'cores' must be positive")

    tmpdir = parameters.get("tmpdir", DEFAULT_TMPDIR)
    partition = parameters.get("partition")
    job_extra_directives = ensure_list(
        parameters.get("job_extra_directives"), "job_extra_directives"
    )
    project = parameters.get("project")
    memory_per_core_property_name = parameters.get("memory_per_core_property_name")
    user_job_script_prologue = ensure_list(
        parameters.get("job_script_prologue"), "job_script_prologue"
    )

    env_exports: List[str] = []
    if "pure_dask" in parameters:
        pure_dask = parse_bool(parameters.get("pure_dask"), "pure_dask")
    else:
        pure_dask = False
    worker_processes_raw = parameters.get("processes")
    if worker_processes_raw is None:
        worker_processes = cores if pure_dask else 1
    else:
        try:
            worker_processes = int(worker_processes_raw)
        except Exception:
            raise RuntimeError("Parameter 'processes' must be an integer")
        if worker_processes <= 0:
            raise RuntimeError("Parameter 'processes' must be positive")

    worker_threads_raw = parameters.get("worker_threads")
    if worker_threads_raw is not None:
        try:
            worker_threads = int(worker_threads_raw)
        except Exception:
            raise RuntimeError("Parameter 'worker_threads' must be an integer")
        if worker_threads <= 0:
            raise RuntimeError("Parameter 'worker_threads' must be positive")
    else:
        if pure_dask:
            worker_threads = 2
        else:
            transformation_throttle = parameters.get(
                "transformation_throttle", DEFAULT_TRANSFORMATION_THROTTLE
            )
            try:
                transformation_throttle = int(transformation_throttle)
            except Exception:
                raise RuntimeError(
                    "Parameter 'transformation_throttle' must be an integer"
                )
            if transformation_throttle <= 0:
                raise RuntimeError(
                    "Parameter 'transformation_throttle' must be positive"
                )

            worker_threads = cores * transformation_throttle
            env_exports.append(
                format_bash_export(
                    "SEAMLESS_WORKER_TRANSFORMATION_THROTTLE", transformation_throttle
                )
            )

    unknown_task_duration = parameters.get(
        "unknown-task-duration", DEFAULT_UNKNOWN_TASK_DURATION
    )
    target_duration = parameters.get("target-duration", DEFAULT_TARGET_DURATION)

    internal_port_range = parameters.get("internal-port-range")
    if internal_port_range is None:
        internal_port_range = port_range
    internal_port_range = normalize_port_range(internal_port_range)
    internal_port_range_str = render_port_range_string(internal_port_range)

    lifetime_stagger = parameters.get("lifetime-stagger", DEFAULT_LIFETIME_STAGGER)
    lifetime_value = parameters.get("lifetime")
    if lifetime_value is None:
        try:
            walltime_td = parse_timedelta_value(walltime)
            stagger_td = parse_timedelta_value(lifetime_stagger)
            grace_td = parse_timedelta_value(DEFAULT_LIFETIME_GRACE)
            lifetime_value = (
                f"{int((walltime_td - stagger_td - grace_td).total_seconds())}s"
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to compute default lifetime: {exc}")
    try:
        lifetime_td = parse_timedelta_value(lifetime_value)
    except Exception as exc:
        raise RuntimeError(f"Invalid 'lifetime' value: {exc}")
    if lifetime_td.total_seconds() <= 0:
        raise RuntimeError("Computed 'lifetime' must be positive")

    dask_resources = parameters.get("dask-resources")
    if dask_resources is not None and not isinstance(dask_resources, Mapping):
        raise RuntimeError("Parameter 'dask-resources' must be a mapping if defined")

    extra_dask_config = parameters.get("extra_dask_config", {})
    if extra_dask_config is None:
        extra_dask_config = {}
    if not isinstance(extra_dask_config, Mapping):
        raise RuntimeError("Parameter 'extra_dask_config' must be a mapping")
    for key, val in extra_dask_config.items():
        if not isinstance(key, str):
            raise RuntimeError("Keys of 'extra_dask_config' must be strings")
        if not isinstance(val, str):
            raise RuntimeError("Values of 'extra_dask_config' must be strings")

    scheduler_port = pick_random_free_port(host, port_range[0], port_range[1])
    dashboard_port = pick_random_free_port(
        host, port_range[0], port_range[1], exclude=(scheduler_port,)
    )

    worker_extra_args = [
        f"--worker-port={internal_port_range_str}",
        f"--nanny-port={internal_port_range_str}",
    ]
    if worker_threads is not None:
        worker_extra_args.insert(0, f"--nthreads {worker_threads}")

    jobqueue_common: Dict[str, Any] = {
        "processes": worker_processes,
        "python": "python",
        "walltime": walltime,
        "cores": cores,
        "memory": memory,
        "local-directory": tmpdir,
        "temp-directory": tmpdir,
        "scheduler-options": {
            "port": scheduler_port,
            "dashboard_address": str(dashboard_port),
            "host": host,
        },
        # KLUDGE
        "worker-extra-args": worker_extra_args,
        # /KLUDGE
    }
    if partition is not None:
        jobqueue_common["queue"] = partition
    if job_extra_directives:
        jobqueue_common["job-extra-directives"] = job_extra_directives
    if project is not None:
        jobqueue_common["project"] = project
    if memory_per_core_property_name is not None:
        jobqueue_common["memory-per-core-property-name"] = memory_per_core_property_name

    base_prologue = list(user_job_script_prologue)

    dask_config_flat: Dict[str, Any] = {
        "distributed.worker.daemon": False,
        "distributed.scheduler.unknown-task-duration": unknown_task_duration,
        "distributed.scheduler.target-duration": target_duration,
        "distributed.worker.port": internal_port_range_str,
        "distributed.nanny.port": internal_port_range_str,
    }
    if lifetime_stagger is not None:
        dask_config_flat["distributed.worker.lifetime.stagger"] = lifetime_stagger
    if lifetime_value is not None:
        dask_config_flat["distributed.worker.lifetime.duration"] = lifetime_value
    if dask_resources is not None:
        dask_config_flat["distributed.worker.resources"] = dict(dask_resources)
    for key, val in extra_dask_config.items():
        dask_config_flat[key] = val

    for key, val in dask_config_flat.items():
        if key.startswith(
            ("distributed.worker.", "distributed.scheduler.", "distributed.nanny.")
        ):
            env_exports.append(format_bash_export(dask_key_to_env_var(key), val))

    jobqueue_config: Dict[str, Dict[str, Any]] = {}
    for system in JOBQUEUE_SYSTEMS:
        prologue = list(base_prologue)
        if system == "slurm":
            if log_handle is not None:
                log_name = log_handle.name + "-%j"
                prologue += [f"#SBATCH --output={log_name}.out"]
                prologue += [f"#SBATCH --error={log_name}.err"]

        prologue += env_exports
        if system == "slurm":
            prologue.append("export PYTHON_CPU_COUNT=$SLURM_JOB_CPUS_PER_NODE")
        config = dict(jobqueue_common)
        config["job-script-prologue"] = prologue
        jobqueue_config[system] = config
        if system == "slurm":
            worker_name = "worker-$SLURM_JOB_ID"
            if "worker-extra-args" not in config:
                config["worker-extra-args"] = []
            worker_extra_args = config["worker-extra-args"]
            worker_extra_args += ["--name", worker_name]

    if "interactive" in parameters:
        interactive = parse_bool(parameters.get("interactive"), "interactive")
    else:
        interactive = False
    maximum_jobs_raw = parameters.get("maximum_jobs")
    maximum_jobs: Optional[int] = None
    if maximum_jobs_raw is not None:
        try:
            maximum_jobs = int(maximum_jobs_raw)
        except Exception:
            raise RuntimeError("Parameter 'maximum_jobs' must be an integer if defined")
        if maximum_jobs <= 0:
            raise RuntimeError("Parameter 'maximum_jobs' must be positive")
    else:
        maximum_jobs = 1  # default: single worker

    if maximum_jobs == 1:
        lifetime_value = None
        lifetime_stagger = None

    return WrapperConfig(
        common=jobqueue_common,
        cores=cores,
        worker_threads=worker_threads,
        worker_processes=worker_processes,
        worker_port_range=internal_port_range_str,
        nanny_port_range=internal_port_range_str,
        worker_lifetime=lifetime_value,
        worker_lifetime_stagger=lifetime_stagger,
        worker_resources=dict(dask_resources) if dask_resources is not None else None,
        jobqueue_config=jobqueue_config,
        dask_config=merge_flat_config(dask_config_flat),
        env_exports=env_exports,
        scheduler_port=scheduler_port,
        dashboard_port=dashboard_port,
        interactive=interactive,
        maximum_jobs=maximum_jobs,
        memory_per_core_property_name=memory_per_core_property_name,
        pure_dask=pure_dask,
    )


def load_cluster_from_string(cluster_string: str, cluster_base: Any):
    from distributed import LocalCluster

    if "::" not in cluster_string:
        raise RuntimeError("Cluster string must be of the form 'MODULE::SYMBOL'")
    module_name, symbol_name = cluster_string.split("::", 1)
    module = import_module(module_name)
    target = getattr(module, symbol_name)
    if isinstance(target, cluster_base):
        if isinstance(target, LocalCluster):
            raise RuntimeError(
                "Cannot return LocalCluster instance: LocalCluster doesn't read ports from config. Return a LocalCluster class or subclass instead."
            )
        return target

    if isinstance(target, FunctionType):
        target = target()
    elif isinstance(target, type):
        if not issubclass(target, cluster_base):
            raise RuntimeError("Cluster class must subclass dask.distributed.Cluster")
        if issubclass(target, LocalCluster):
            return target
        target = target()
    else:
        raise RuntimeError(
            "Cluster symbol must be a Cluster instance, class or function"
        )

    if isinstance(target, LocalCluster):
        raise RuntimeError(
            "Cannot return LocalCluster instance: LocalCluster doesn't read ports from config. Return a LocalCluster class or subclass instead."
        )
    if not isinstance(target, cluster_base):
        raise RuntimeError("Cluster string did not yield a Dask Cluster instance")
    return target


def _scheduler_activity(dask_scheduler, monitor_id: str):
    client_count = len([cid for cid in dask_scheduler.clients if cid != monitor_id])
    worker_count = len(getattr(dask_scheduler, "workers", {}))
    active_tasks = [
        ts
        for ts in dask_scheduler.tasks.values()
        if getattr(ts, "state", None) not in ("released", "forgotten")
    ]
    return {
        "client_count": client_count,
        "worker_count": worker_count,
        "task_count": len(active_tasks),
    }


def _dummy_task():
    return None


def _submit_dummy_task(client, label: str):
    try:
        key = f"seamless-dask-wrapper-dummy-{label}-{time.time_ns()}"
        return client.submit(_dummy_task, pure=False, key=key)
    except Exception:
        return None


def keep_cluster_alive(
    cluster,
    timeout: Optional[float],
    *,
    is_local: bool,
    target_workers: int,
    interactive: bool,
    adaptive_settings: Optional[Mapping[str, Any]] = None,
):
    from distributed import Client
    import warnings

    monitor_client = Client(
        cluster, set_as_default=False, name="seamless-dask-wrapper-monitor"
    )
    last_activity: Optional[float] = None
    last_recovery_attempt: Optional[float] = None
    no_worker_since: Optional[float] = None
    saw_worker: bool = False
    dummy_futures: List[Any] = []
    try:
        if interactive and not is_local:
            future = _submit_dummy_task(monitor_client, "startup")
            if future is not None:
                dummy_futures.append(future)
        while True:
            try:
                activity = monitor_client.run_on_scheduler(
                    _scheduler_activity, monitor_id=monitor_client.id
                )
                has_activity = bool(
                    activity.get("client_count", 0)
                    > 1  # a monitor client is a client too!
                    or activity.get("task_count", 0) > 0
                )
            except Exception:
                activity = {}
                has_activity = False
            if not is_local:
                now = time.monotonic()
                worker_count = int(activity.get("worker_count", 0) or 0)
                task_count = max(
                    int(activity.get("task_count", 0) or 0) - len(dummy_futures), 0
                )
                if worker_count > 0:
                    saw_worker = True
                if worker_count == 0 and task_count > 0:
                    if not saw_worker:
                        no_worker_since = None
                        time.sleep(INACTIVITY_CHECK_INTERVAL)
                        continue
                    if no_worker_since is None:
                        no_worker_since = now
                else:
                    no_worker_since = None
                stuck = bool(
                    no_worker_since is not None
                    and (now - no_worker_since) >= WORKER_STUCK_TIMEOUT
                )
                if worker_count == 0 and task_count > 0:
                    if (
                        last_recovery_attempt is None
                        or now - last_recovery_attempt >= WORKER_RECOVERY_INTERVAL
                    ):
                        last_recovery_attempt = now
                        if adaptive_settings is not None:
                            try:
                                adapt = getattr(cluster, "adapt", None)
                                if callable(adapt):
                                    adapt(**adaptive_settings)
                            except Exception:
                                pass
                        try:
                            warnings.warn(
                                "[seamless-dask-wrapper] Scheduler has pending tasks but no workers; forcing jobqueue rescale",
                                RuntimeWarning,
                            )
                            correct_state = getattr(cluster, "_correct_state", None)
                            if callable(correct_state):
                                sync = getattr(cluster, "sync", None)
                                if callable(sync):
                                    sync(correct_state)
                                else:
                                    correct_state()
                        except Exception:
                            pass
                        try:
                            scale = getattr(cluster, "scale", None)
                            if callable(scale):
                                if interactive or stuck:
                                    scale(0)
                                    no_worker_since = None
                                scale(max(target_workers, 1))
                        except Exception:
                            pass
                        if interactive:
                            future = _submit_dummy_task(monitor_client, "recovery")
                            if future is not None:
                                dummy_futures.append(future)
            if has_activity:
                last_activity = None
            else:
                if last_activity is None:
                    last_activity = time.monotonic()
                else:
                    idle_for = time.monotonic() - last_activity
                    if timeout is not None and idle_for >= timeout:
                        break
                    if is_local:
                        try:
                            current = getattr(cluster, "workers", None)
                            worker_count = len(current) if current is not None else 0
                            if worker_count == 0:
                                try:
                                    warnings.warn(
                                        "[seamless-dask-wrapper] LocalCluster reports 0 workers; attempting restart/scale",
                                        RuntimeWarning,
                                    )
                                    restart = getattr(cluster, "restart", None)
                                    if callable(restart):
                                        restart()
                                    else:
                                        scale = getattr(cluster, "scale", None)
                                        if callable(scale):
                                            scale(max(target_workers, 1))
                                except Exception:
                                    pass
                        except Exception:
                            pass
            if dummy_futures:
                dummy_futures = [fut for fut in dummy_futures if not fut.done()]
            time.sleep(INACTIVITY_CHECK_INTERVAL)
    finally:
        try:
            monitor_client.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Launch Seamless Dask wrapper")
    parser.add_argument(
        "cluster", type=str, help="Cluster string in the form MODULE::SYMBOL"
    )
    parser.add_argument(
        "--port-range",
        type=int,
        nargs=2,
        required=True,
        metavar=("START", "END"),
        help="Inclusive port range to select random scheduler/dashboard ports from",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Scheduler host")
    parser.add_argument(
        "--status-file",
        required=True,
        help="JSON status file to read parameters from and write status to",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        help="Stop the wrapper after this many seconds of inactivity",
    )
    args = parser.parse_args()

    global status_tracker
    status_file_contents = wait_for_status_file(args.status_file)
    configure_log_handle(status_file_contents)
    parameters = status_file_contents.get("parameters", {}) or {}
    status_tracker = StatusFileTracker(args.status_file, status_file_contents)

    if args.timeout is not None and args.timeout <= 0:
        raise_startup_error(RuntimeError("--timeout must be a positive number"))

    try:
        wrapper_config = build_wrapper_configuration(
            host=args.host,
            port_range=(args.port_range[0], args.port_range[1]),
            parameters=parameters,
        )
    except BaseException as exc:
        raise_startup_error(exc)

    try:
        log_print("Dask worker configuration:")
        log_print(f"cores: {wrapper_config.cores}")
        log_print(f"processes: {wrapper_config.worker_processes}")
        if wrapper_config.worker_threads is None:
            log_print("worker_threads: jobqueue default")
        else:
            log_print(f"worker_threads: {wrapper_config.worker_threads}")
        log_print(f"pure_dask: {wrapper_config.pure_dask}")

        import dask

        # Merge into the existing config to avoid clobbering distributed defaults
        dask.config.update(
            dask.config.config, {"jobqueue": wrapper_config.jobqueue_config}
        )
        for modname in sys.modules.keys():
            assert not modname.startswith("distributed"), modname
        dask.config.update(dask.config.config, wrapper_config.dask_config)

        from distributed.deploy.cluster import Cluster
        from distributed import LocalCluster

    except BaseException as exc:
        raise_startup_error(exc)

    try:
        cluster = load_cluster_from_string(args.cluster, Cluster)

        if isinstance(cluster, type) and issubclass(cluster, LocalCluster):
            if wrapper_config.maximum_jobs == 1:
                assert wrapper_config.worker_lifetime is None
                assert wrapper_config.worker_lifetime_stagger is None
                # KLUDGE
                # Observing weird worker retiring behavior, shouldn't happen...
                # This should squash it (on top of the restart in the keep cluster alive)
                wrapper_config.worker_lifetime = "24h"
                wrapper_config.worker_lifetime_stagger = "10m"
                # /KLUDGE
            cluster_kwargs = {
                "n_workers": wrapper_config.maximum_jobs,
                "host": wrapper_config.common["scheduler-options"]["host"],
                "scheduler_port": wrapper_config.scheduler_port,
                "dashboard_address": ":" + str(wrapper_config.dashboard_port),
                "worker_port": wrapper_config.worker_port_range,
                "port": wrapper_config.nanny_port_range,
                "lifetime": wrapper_config.worker_lifetime,
                "lifetime_stagger": wrapper_config.worker_lifetime_stagger,
                "resources": wrapper_config.worker_resources,
                # also not read from config: scheduler protocol and security
            }
            if wrapper_config.worker_threads is not None:
                cluster_kwargs["threads_per_worker"] = wrapper_config.worker_threads
            cluster = cluster(**cluster_kwargs)
        else:
            try:
                from dask_jobqueue import OARCluster  # type: ignore
            except Exception:
                OARCluster = None
            if OARCluster is not None and isinstance(cluster, OARCluster):
                if wrapper_config.memory_per_core_property_name is None:
                    raise RuntimeError(
                        "Parameter 'memory_per_core_property_name' is required for OARCluster"
                    )

        assert isinstance(cluster, Cluster)
        if not isinstance(cluster, LocalCluster):
            log_print(
                "adapt cluster:",
                int(wrapper_config.interactive),
                wrapper_config.maximum_jobs,
            )
            adaptive_settings = {
                "minimum_jobs": int(wrapper_config.interactive),
                "maximum_jobs": wrapper_config.maximum_jobs,
                "target_duration": parameters.get(
                    "target-duration", DEFAULT_TARGET_DURATION
                ),
                "wait_count": ADAPTIVE_WAIT_COUNT,
                "interval": ADAPTIVE_INTERVAL,
            }
            cluster.adapt(**adaptive_settings)
        else:
            adaptive_settings = None

        status_tracker.write_running(
            wrapper_config.scheduler_port, wrapper_config.dashboard_port
        )

        print_script = getattr(cluster, "job_script", None)
        if callable(print_script):
            try:
                job_script = print_script()
            except Exception as exc:
                log_print(f"Failed to generate job script: {exc}")
            else:
                label = (
                    "SLURM job script:"
                    if cluster.__class__.__name__ == "SLURMCluster"
                    else "Job script:"
                )
                log_print(label)
                log_print(job_script.rstrip())

        log_print("Dask scheduler address:")
        log_print(cluster.scheduler_address)

        log_print("Dask dashboard address:")
        log_print(cluster.dashboard_link)

    except BaseException as exc:
        raise_startup_error(exc)

    def raise_system_exit(*_args, **_kwargs):
        raise SystemExit

    signal.signal(signal.SIGTERM, raise_system_exit)
    signal.signal(signal.SIGHUP, raise_system_exit)
    signal.signal(signal.SIGINT, raise_system_exit)

    try:
        keep_cluster_alive(
            cluster,
            args.timeout,
            is_local=isinstance(cluster, LocalCluster),
            target_workers=wrapper_config.maximum_jobs,
            interactive=wrapper_config.interactive,
            adaptive_settings=adaptive_settings,
        )
    finally:
        try:
            log_print("Shutdown after timeout")
            cluster.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
