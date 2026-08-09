"""Deployment wrapper; keeps the existing scanner app.py unchanged."""

from app import app

from nifty_drop_monitor import NiftyDropMonitor

nifty_monitor = NiftyDropMonitor()
nifty_monitor.start()


@app.get("/api/nifty-status")
def nifty_status():
    return nifty_monitor.status
