# Copilot Instructions

## General Guidelines
- User prefers very simple, deterministic implementations ('simple stupid') over complex orchestration for polling logic.
- Implement watchdog behavior to avoid restarts; instead, continue polling by skipping problematic items and moving on.
- Terminate/killing of possibly-running poller instances should be performed by the Poller server on startup (not by the GUI). Implemented: moved kill logic from GUI to server startup.