# hec/core/scheduler_setup.py
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

logger = logging.getLogger(__name__)


def setup_scheduler(config: dict, run_in_background: bool = False):
    """
    Initializes and configures the APScheduler.

    Args:
        config (dict): Application configuration.
        run_in_background (bool): If True use BackgroundScheduler, otherwise BlockingScheduler.

    Returns:
        APScheduler instance (BlockingScheduler or BackgroundScheduler).
    """
    scheduler_config = config.get('scheduler', {})  # Get scheduler specific config

    # Define executors
    executors = {
        'default': ThreadPoolExecutor(scheduler_config.get('thread_pool_max_workers', 10)),
    }

    job_defaults = {
        'coalesce': scheduler_config.get('coalesce_jobs', True),
        'max_instances': scheduler_config.get('max_instances_per_job', 3),
        'misfire_grace_time': scheduler_config.get('misfire_grace_time_seconds', 60)
    }

    if run_in_background:
        logger.info("Initializing BackgroundScheduler.")
        scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults,
                                        timezone=scheduler_config.get('timezone', 'Europe/Brussels'))
    else:
        logger.info("Initializing BlockingScheduler.")
        scheduler = BlockingScheduler(executors=executors, job_defaults=job_defaults,
                                      timezone=scheduler_config.get('timezone', 'Europe/Brussels'))

    logger.info(f"APScheduler initialized with timezone: {scheduler.timezone}")
    return scheduler
