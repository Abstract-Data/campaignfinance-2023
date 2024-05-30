# progress_display.py
from time import sleep
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.table import Table


class ProgressDisplay:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ProgressDisplay, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self.job_progress = Progress(
            "{task.description}",
            SpinnerColumn(),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        )
        self.overall_progress = Progress()
        self.progress_table = Table.grid()

    def add_task_to_job_progress(self, description: str, total: int):
        return self.job_progress.add_task(description, total=total)

    def add_task_to_overall_progress(self, description: str, total: int):
        return self.overall_progress.add_task(description, total=total)

    def update_progress_table(self):
        self.progress_table.add_row(
            Panel.fit(
                self.overall_progress, title="Overall Progress", border_style="green", padding=(2, 2)
            ),
            Panel.fit(self.job_progress, title="[b]Jobs", border_style="red", padding=(1, 2)),
        )
    def start_live_progress(self):
        with Live(self.progress_table, refresh_per_second=10):
            while not self.overall_progress.finished:
                sleep(0.1)
                for job in self.job_progress.tasks:
                    if not job.finished:
                        self.job_progress.advance(job.id)

                completed = sum(task.completed for task in self.job_progress.tasks)
                self.overall_progress.update(self.overall_task, completed=completed)