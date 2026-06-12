import logging
import random
import signal
import sys
import threading
import time
from types import FrameType
from typing import Optional

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.db.utils import OperationalError

from custom_code.tasks import run_observation_status_update
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
        self.next_status_enqueue_at = 0.0 if status_interval else None
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

    def start(self) -> None:
        if self.configure_signal_handlers:
            self.configure_signals()
        logger.info(
            "Starting BHTOM %s for queues=%s",
            self.worker_name,
            ",".join(self.queue_names),
        )

        if self.startup_delay and self.interval:
            time.sleep(random.random())

        while self.running:
            self.run_due_status_update()

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

        try:
            task = db_task_result.task
            task_result = db_task_result.task_result

            logger.info(
                "Task id=%s path=%s state=%s",
                db_task_result.id,
                db_task_result.task_path,
                task_result.status,
            )
            return_value = task.call(*task_result.args, **task_result.kwargs)
            db_task_result.set_succeeded(return_value)
            task_finished.send(
                sender=type(task.get_backend()), task_result=db_task_result.task_result
            )
        except BaseException as exc:
            logger.exception(
                "Task id=%s path=%s failed",
                db_task_result.id,
                getattr(db_task_result, "task_path", None),
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

        if not package_logger.hasHandlers():
            package_logger.addHandler(logging.StreamHandler(self.stdout))
        if not logger.hasHandlers():
            logger.addHandler(logging.StreamHandler(self.stdout))

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
        workers: int,
        **options,
    ) -> None:
        self.configure_logging(verbosity)
        worker_count = max(1, int(workers))
        status_interval = max(0, int(status_interval))
        queue_names = queue_name.split(",")
        logger.info(
            "Configured BHTOM db_worker workers=%s queues=%s status_interval=%s bhtom2_token_configured=%s bhtom2_upload_url_configured=%s",
            worker_count,
            ",".join(queue_names),
            status_interval,
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
