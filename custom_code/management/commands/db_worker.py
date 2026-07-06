import logging
import os
import random
import signal
import sys
import threading
import time
from datetime import timedelta
from types import FrameType
from typing import Optional

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.db.utils import OperationalError
from django.utils import timezone
from tom_targets.models import Target

from custom_code.tasks import enqueue_target_dataservices_update, run_observation_status_update
from django_tasks import DEFAULT_TASK_BACKEND_ALIAS
from django_tasks.backends.database.management.commands.db_worker import (
    package_logger,
    valid_backend_name,
    valid_interval,
)
from django_tasks.backends.database.models import DBTaskResult
from django_tasks.task import DEFAULT_QUEUE_NAME


logger = logging.getLogger("custom_code.bhtom_db_worker")


class CompatibleExclusiveTransaction:
    def __init__(self, using):
        self.using = using
        self.connection = None
        self.cursor = None
        self.manual = False
        self.atomic = None

    def __enter__(self):
        self.connection = transaction.get_connection(self.using)
        transaction_mode = getattr(self.connection, "transaction_mode", None)
        self.manual = self.connection.vendor == "sqlite" and transaction_mode != "EXCLUSIVE"

        if self.manual:
            self.cursor = self.connection.cursor()
            self.cursor.execute("BEGIN EXCLUSIVE")
            return self

        self.atomic = transaction.atomic(using=self.using)
        return self.atomic.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        if self.manual:
            try:
                if exc_type is None:
                    self.cursor.execute("COMMIT")
                else:
                    self.cursor.execute("ROLLBACK")
            finally:
                self.cursor.close()
            return False

        return self.atomic.__exit__(exc_type, exc_value, traceback)


class ScheduledStatusWorker:
    def __init__(
        self,
        *,
        queue_names,
        interval,
        batch,
        backend_name,
        startup_delay,
        status_interval,
        dataservices_interval,
        dataservices_importance_gt,
        configure_signal_handlers=True,
        worker_name=None,
    ):
        self.queue_names = queue_names
        self.process_all_queues = "*" in queue_names
        self.interval = interval
        self.batch = batch
        self.backend_name = backend_name
        self.startup_delay = startup_delay
        self.status_interval = status_interval
        self.dataservices_interval = dataservices_interval
        self.dataservices_importance_gt = dataservices_importance_gt
        self.next_status_enqueue_at = 0.0 if status_interval else None
        self.next_dataservices_enqueue_at = 0.0 if dataservices_interval else None
        self.heartbeat_interval = getattr(settings, "DB_WORKER_HEARTBEAT_INTERVAL", 300)
        self.stale_running_after = getattr(settings, "DB_WORKER_STALE_RUNNING_AFTER", 7200)
        self.next_heartbeat_at = 0.0
        self.next_stale_recovery_at = 0.0
        self.configure_signal_handlers = configure_signal_handlers
        self.worker_name = worker_name or "worker"

        self.running = True
        self.running_task = False

    def stop(self) -> None:
        self.running = False

    def shutdown(self, signum: int, frame: Optional[FrameType]) -> None:
        if not self.running:
            logger.warning(
                "Received %s - terminating current task.", signal.strsignal(signum)
            )
            sys.exit(1)

        logger.warning(
            "Received %s - shutting down gracefully... (press Ctrl+C again to force)",
            signal.strsignal(signum),
        )
        self.running = False

        if not self.running_task:
            sys.exit(0)

    def configure_signals(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        if hasattr(signal, "SIGQUIT"):
            signal.signal(signal.SIGQUIT, self.shutdown)

    def run_due_status_update(self) -> None:
        if self.next_status_enqueue_at is None:
            return
        now = time.monotonic()
        if now < self.next_status_enqueue_at:
            return
        self.next_status_enqueue_at = now + self.status_interval
        try:
            logger.info("Starting scheduled observation status update.")
            result = run_observation_status_update()
        except Exception:
            logger.exception("Scheduled observation status update failed.")
            return
        logger.info(
            "Finished scheduled observation status update; next update in %s seconds: %s",
            self.status_interval,
            result,
        )

    def run_due_dataservices_update(self) -> None:
        if self.next_dataservices_enqueue_at is None:
            return
        now = time.monotonic()
        if now < self.next_dataservices_enqueue_at:
            return
        self.next_dataservices_enqueue_at = now + self.dataservices_interval

        try:
            target_ids = list(
                Target.objects
                .filter(importance__gt=self.dataservices_importance_gt)
                .order_by("pk")
                .values_list("pk", flat=True)
            )
            for target_id in target_ids:
                enqueue_target_dataservices_update(
                    target_id,
                    include_create_only=False,
                    force_all_services=False,
                )
        except Exception:
            logger.exception("Scheduled DataServices refresh enqueue failed.")
            return

        logger.info(
            "Scheduled DataServices refresh enqueued targets=%s importance_gt=%s next_update_in=%s seconds.",
            len(target_ids),
            self.dataservices_importance_gt,
            self.dataservices_interval,
        )

    def log_heartbeat(self) -> None:
        if not self.heartbeat_interval:
            return
        now = time.monotonic()
        if now < self.next_heartbeat_at:
            return
        self.next_heartbeat_at = now + self.heartbeat_interval
        try:
            tasks = DBTaskResult.objects.filter(backend_name=self.backend_name)
            if not self.process_all_queues:
                tasks = tasks.filter(queue_name__in=self.queue_names)
            counts = {}
            for row in tasks.values("status"):
                status = row.get("status")
                counts[status] = counts.get(status, 0) + 1
        except Exception:
            logger.exception("BHTOM %s heartbeat failed while counting tasks.", self.worker_name)
            return
        logger.info(
            "BHTOM %s heartbeat pid=%s queues=%s running_task=%s task_counts=%s",
            self.worker_name,
            os.getpid(),
            ",".join(self.queue_names),
            self.running_task,
            counts,
        )

    def recover_stale_running_tasks(self) -> None:
        if not self.stale_running_after:
            return
        now = time.monotonic()
        check_interval = min(max(self.stale_running_after / 2, 60), 300)
        if now < self.next_stale_recovery_at:
            return
        self.next_stale_recovery_at = now + check_interval

        cutoff = timezone.now() - timedelta(seconds=self.stale_running_after)
        try:
            stale_tasks = DBTaskResult.objects.filter(
                backend_name=self.backend_name,
                status="RUNNING",
                started_at__lt=cutoff,
                finished_at__isnull=True,
            )
            if not self.process_all_queues:
                stale_tasks = stale_tasks.filter(queue_name__in=self.queue_names)
            stale_ids = list(stale_tasks.values_list("id", flat=True)[:100])
            if not stale_ids:
                return
            recovered = DBTaskResult.objects.filter(
                id__in=stale_ids,
                status="RUNNING",
                finished_at__isnull=True,
            ).update(status="NEW", started_at=None)
        except Exception:
            logger.exception("BHTOM %s stale RUNNING task recovery failed.", self.worker_name)
            return

        logger.warning(
            "Recovered stale RUNNING tasks older_than=%ss recovered=%s task_ids=%s",
            self.stale_running_after,
            recovered,
            stale_ids,
        )

    def start(self) -> None:
        if self.configure_signal_handlers:
            self.configure_signals()
        logger.info(
            "Starting BHTOM %s pid=%s queues=%s backend=%s poll_interval=%s batch=%s heartbeat_interval=%s stale_running_after=%s data_service_timeout=(%s,%s) data_service_job_timeout=%s",
            self.worker_name,
            os.getpid(),
            ",".join(self.queue_names),
            self.backend_name,
            self.interval,
            self.batch,
            self.heartbeat_interval,
            self.stale_running_after,
            getattr(settings, "DATA_SERVICE_CONNECT_TIMEOUT", 10),
            getattr(settings, "DATA_SERVICE_READ_TIMEOUT", 60),
            getattr(settings, "DATA_SERVICE_JOB_TIMEOUT", 300),
        )

        if self.startup_delay and self.interval:
            time.sleep(random.random())

        while self.running:
            self.log_heartbeat()
            self.recover_stale_running_tasks()
            self.run_due_status_update()
            self.run_due_dataservices_update()

            tasks = DBTaskResult.objects.ready().filter(backend_name=self.backend_name)
            if not self.process_all_queues:
                tasks = tasks.filter(queue_name__in=self.queue_names)

            task_result = None
            try:
                self.running_task = True

                with CompatibleExclusiveTransaction(tasks.db):
                    try:
                        task_result = tasks.get_locked()
                    except OperationalError as exc:
                        if "is locked" in exc.args[0]:
                            task_result = None
                        else:
                            raise

                    if task_result is not None:
                        task_result.claim()

                if task_result is not None:
                    self.run_task(task_result)

            finally:
                self.running_task = False

                for conn in connections.all(initialized_only=True):
                    conn.close()

            if self.batch and task_result is None:
                return

            if self.running and not task_result:
                time.sleep(self.interval)

    def run_task(self, db_task_result: DBTaskResult) -> None:
        from django.core.exceptions import SuspiciousOperation
        from django_tasks.signals import task_finished

        started_at = time.monotonic()
        try:
            task = db_task_result.task
            task_result = db_task_result.task_result
            args = task_result.args
            kwargs = task_result.kwargs
            target_id = args[0] if args and "dataservice" in db_task_result.task_path else None
            service_name = args[1] if len(args) > 1 and "dataservice" in db_task_result.task_path else None

            logger.info(
                "Task id=%s path=%s state=%s target_id=%s service=%s starting",
                db_task_result.id,
                db_task_result.task_path,
                task_result.status,
                target_id,
                service_name,
            )
            return_value = task.call(*args, **kwargs)
            db_task_result.set_succeeded(return_value)
            task_finished.send(
                sender=type(task.get_backend()), task_result=db_task_result.task_result
            )
            logger.info(
                "Task id=%s path=%s target_id=%s service=%s succeeded elapsed=%.2fs",
                db_task_result.id,
                db_task_result.task_path,
                target_id,
                service_name,
                time.monotonic() - started_at,
            )
        except BaseException as exc:
            logger.exception(
                "Task id=%s path=%s failed elapsed=%.2fs exception=%s",
                db_task_result.id,
                getattr(db_task_result, "task_path", None),
                time.monotonic() - started_at,
                exc.__class__.__name__,
            )
            db_task_result.set_failed(exc)
            try:
                sender = type(db_task_result.task.get_backend())
                task_result = db_task_result.task_result
            except (ModuleNotFoundError, SuspiciousOperation):
                logger.exception("Task id=%s failed unexpectedly", db_task_result.id)
            else:
                task_finished.send(sender=sender, task_result=task_result)


class Command(BaseCommand):
    help = "Run the database worker and refresh observation statuses periodically."

    def add_arguments(self, parser):
        parser.add_argument(
            "--queue-name",
            nargs="?",
            default=DEFAULT_QUEUE_NAME,
            type=str,
            help="The queues to process. Separate multiple with a comma. To process all queues, use '*' (default: %(default)r)",
        )
        parser.add_argument(
            "--interval",
            nargs="?",
            default=1,
            type=valid_interval,
            help="The interval in seconds to wait when there are no ready tasks (default: %(default)r)",
        )
        parser.add_argument(
            "--batch",
            action="store_true",
            help="Process all outstanding tasks, then exit",
        )
        parser.add_argument(
            "--backend",
            nargs="?",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            type=valid_backend_name,
            dest="backend_name",
            help="The backend to operate on (default: %(default)r)",
        )
        parser.add_argument(
            "--no-startup-delay",
            action="store_false",
            dest="startup_delay",
            help="Don't add a small delay at startup.",
        )
        parser.add_argument(
            "--status-interval",
            type=int,
            default=getattr(settings, "OBSERVATION_STATUS_UPDATE_INTERVAL_SECONDS", 180),
            help="Seconds between observation status refresh jobs. Use 0 to disable (default: 180).",
        )
        parser.add_argument(
            "--dataservices-interval",
            type=int,
            default=getattr(settings, "DATA_SERVICES_UPDATE_INTERVAL_SECONDS", 86400),
            help="Seconds between scheduled DataServices refresh enqueues. Use 0 to disable (default: 86400).",
        )
        parser.add_argument(
            "--dataservices-importance-gt",
            type=float,
            default=getattr(settings, "DATA_SERVICES_UPDATE_IMPORTANCE_GT", 0.0),
            help="Refresh only targets with importance greater than this value (default: 0).",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=getattr(settings, "DB_WORKER_THREADS", 4),
            help="Number of worker threads to run in this process (default: 4).",
        )

    def configure_logging(self, verbosity: int) -> None:
        if verbosity == 0:
            package_logger.setLevel(logging.CRITICAL)
            logger.setLevel(logging.CRITICAL)
        elif verbosity == 1:
            package_logger.setLevel(logging.WARNING)
            logger.setLevel(logging.INFO)
        elif verbosity == 2:
            package_logger.setLevel(logging.INFO)
            logger.setLevel(logging.INFO)
        else:
            package_logger.setLevel(logging.DEBUG)
            logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        if not package_logger.hasHandlers():
            handler = logging.StreamHandler(self.stdout)
            handler.setFormatter(formatter)
            package_logger.addHandler(handler)
        if not logger.hasHandlers():
            handler = logging.StreamHandler(self.stdout)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        log_dir = os.path.join(settings.BASE_DIR, "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "dbworker.out.log")
        existing_file_paths = {
            getattr(handler, "baseFilename", None)
            for handler in logger.handlers
        }
        if log_path not in existing_file_paths:
            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            package_logger.addHandler(file_handler)

    def handle(
        self,
        *,
        verbosity: int,
        queue_name: str,
        interval: float,
        batch: bool,
        backend_name: str,
        startup_delay: bool,
        status_interval: int,
        dataservices_interval: int,
        dataservices_importance_gt: float,
        workers: int,
        **options,
    ) -> None:
        self.configure_logging(verbosity)
        worker_count = max(1, int(workers))
        status_interval = max(0, int(status_interval))
        dataservices_interval = max(0, int(dataservices_interval))
        dataservices_importance_gt = float(dataservices_importance_gt)
        queue_names = queue_name.split(",")
        logger.info(
            "Configured BHTOM db_worker workers=%s queues=%s status_interval=%s dataservices_interval=%s dataservices_importance_gt=%s bhtom2_token_configured=%s bhtom2_upload_url_configured=%s",
            worker_count,
            ",".join(queue_names),
            status_interval,
            dataservices_interval,
            dataservices_importance_gt,
            bool(str(getattr(settings, "BHTOM2_API_TOKEN", "") or "").strip()),
            bool(str(getattr(settings, "BHTOM2_UPLOAD_SERVICE_URL", "") or "").strip()),
        )

        if worker_count == 1:
            worker = ScheduledStatusWorker(
                queue_names=queue_names,
                interval=interval,
                batch=batch,
                backend_name=backend_name,
                startup_delay=startup_delay,
                status_interval=status_interval,
                dataservices_interval=dataservices_interval,
                dataservices_importance_gt=dataservices_importance_gt,
            )
            worker.start()

            if batch:
                logger.info("No more tasks to run - exiting gracefully.")
            return

        workers_list = [
            ScheduledStatusWorker(
                queue_names=queue_names,
                interval=interval,
                batch=batch,
                backend_name=backend_name,
                startup_delay=startup_delay,
                status_interval=status_interval if index == 0 else 0,
                dataservices_interval=dataservices_interval if index == 0 else 0,
                dataservices_importance_gt=dataservices_importance_gt,
                configure_signal_handlers=False,
                worker_name=f"worker-{index + 1}",
            )
            for index in range(worker_count)
        ]

        def shutdown(signum: int, frame: Optional[FrameType]) -> None:
            logger.warning(
                "Received %s - shutting down %s workers gracefully...",
                signal.strsignal(signum),
                worker_count,
            )
            for scheduled_worker in workers_list:
                scheduled_worker.stop()

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)
        if hasattr(signal, "SIGQUIT"):
            signal.signal(signal.SIGQUIT, shutdown)

        threads = [
            threading.Thread(
                target=scheduled_worker.start,
                name=scheduled_worker.worker_name,
            )
            for scheduled_worker in workers_list
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        if batch:
            logger.info("No more tasks to run - exiting gracefully.")
