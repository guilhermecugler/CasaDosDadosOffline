"""Utility helpers for the GUI."""

def progress_bar_update(progress_bar, step):
    """Update the progress bar and force UI refresh."""
    progress_bar.set(step)
    progress_bar.update_idletasks()

