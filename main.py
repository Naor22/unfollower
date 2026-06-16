"""CLI runner — uses the same Bot logic as the web UI."""

import time

import bot


def main() -> int:
    state = bot.StateManager()
    worker = bot.Bot(state)

    last_status = None
    last_detail = None
    last_target = None

    worker.start()
    try:
        while worker.is_running:
            snap = state.snapshot()
            if snap["status"] != last_status or snap["phase_detail"] != last_detail:
                print(f"[{snap['status']}] {snap['phase_detail']}")
                last_status = snap["status"]
                last_detail = snap["phase_detail"]
            if snap["current_target"] and snap["current_target"] != last_target:
                print(
                    f"  → @{snap['current_target']} "
                    f"({snap['progress_index']}/{snap['total_targets']})"
                )
                last_target = snap["current_target"]
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[ctrl-c] stopping...")
        worker.stop()
        worker.join(timeout=30)

    final = state.snapshot()
    print(
        f"[done] status={final['status']} "
        f"unfollowed={final['unfollowed_count']} failed={final['failed_count']}"
    )
    if final["error"]:
        print(f"[error] {final['error']}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
