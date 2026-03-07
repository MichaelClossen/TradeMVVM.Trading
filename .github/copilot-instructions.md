# Copilot Instructions

## General Guidelines
- User prefers very simple, deterministic implementations ('simple stupid') over complex orchestration for polling logic.
- Implement watchdog behavior to avoid restarts; instead, continue polling by skipping problematic items and moving on.