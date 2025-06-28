import asyncio
from typing import Optional
from main import main 
import pdb

class TaskManager:
    def __init__(self):
        self.task: Optional[asyncio.Task] = None
        self.current_input: list[tuple[str, str, int]] = None

    async def start(self, initial_input: list[tuple[str, str, int]]):
        """Starts the background task if it's not already running."""
        if self.task and not self.task.done():
            print("Task is already running. Cannot start again.")
            return

        self.current_input = initial_input
        # Create the task and store a reference to it
        print(f"Creating task with {self.current_input}")
        self.task = asyncio.create_task(main(self.current_input))

    async def stop(self):
        """Stops the background task gracefully."""
        if not self.task or self.task.done():
            print("Task is not running.")
            return

        # Cancel the task
        self.task.cancel()
        # Wait for the task to acknowledge the cancellation
        try:
            await self.task
        except asyncio.CancelledError:
            pass  # Expected exception

        self.task = None
        print("Manager has confirmed the task is stopped.")

    async def restart(self, new_input: list[tuple[str, str, int]] | None):
        """Restarts the background task with a new input parameter."""
        if new_input is None:
            new_input = self.current_input
        print(f"--- Received restart command with new input: '{new_input}' ---")
        await self.stop()
        await self.start(new_input)

    def get_status(self):
        """Returns the current status of the worker."""
        if self.task and not self.task.done():
            is_running = True
        else:
            is_running = False
        return {"is_running": is_running, "current_input": self.current_input}