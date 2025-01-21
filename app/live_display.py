from rich.live import Live
from rich.table import Table
from rich.console import Console
from typing import Dict, Optional
import threading
from datetime import datetime
import queue

class ProgressTracker:
    def __init__(self):
        self._tasks: Dict[str, dict] = {}
        self._lock = threading.Lock()
        self._live: Optional[Live] = None
        self._update_queue = queue.Queue()
        self._console = Console(force_terminal=True, color_system="auto")
        self._running = True
        self._update_thread = threading.Thread(target=self._update_display)
        self._update_thread.daemon = True
        self._update_thread.start()

    def start(self):
        """Start the live display"""
        if not self._live:
            self._live = Live(self._generate_table(), refresh_per_second=4)
            self._live.start()

    def stop(self):
        """Stop the live display and cleanup"""
        self._running = False
        if self._live:
            self._live.stop()
            self._live = None

    def add_task(self, task_id: str, description: str, status: str = "pending"):
        """Add a new task to track"""
        with self._lock:
            self._tasks[task_id] = {
                "description": description,
                "status": status,
                "start_time": datetime.now(),
                "update_time": datetime.now()
            }
        self._queue_update()
        return task_id

    def update_task(self, task_id: str, status: str):
        """Update the status of an existing task"""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id]["status"] = status
                self._tasks[task_id]["update_time"] = datetime.now()
        self._queue_update()

    def remove_task(self, task_id: str):
        """Remove a task from tracking"""
        with self._lock:
            self._tasks.pop(task_id, None)
        self._queue_update()

    def print(self, msg: str):
        """Print a message to the console"""
        self._console.print(msg)

    def _generate_table(self) -> Table:
        """Generate the Rich table for display"""
        table = Table()
        table.add_column("Task ID")
        table.add_column("Description")
        table.add_column("Status")
        table.add_column("Duration")

        with self._lock:
            for task_id, task in self._tasks.items():
                duration = datetime.now() - task["start_time"]
                duration_str = str(duration).split('.')[0]  # Remove microseconds

                # Color-code status
                status_style = {
                    "pending": "yellow",
                    "running": "blue",
                    "completed": "green",
                    "failed": "red"
                }.get(task["status"].lower(), "white")

                table.add_row(
                    task_id,
                    task["description"],
                    f"[{status_style}]{task['status']}[/{status_style}]",
                    duration_str
                )

        return table

    def _queue_update(self):
        """Queue a display update"""
        self._update_queue.put(True)

    def _update_display(self):
        """Update display thread"""
        while self._running:
            try:
                self._update_queue.get(timeout=0.25)
                if self._live:
                    self._live.update(self._generate_table())
            except queue.Empty:
                continue