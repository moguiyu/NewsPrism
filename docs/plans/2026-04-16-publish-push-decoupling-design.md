# Publish/Push Decoupling Design

**Date:** 2026-04-16
**Status:** Approved
**Author:** Claude (brainstorming session)

## Context

The current NewsPrism architecture tightly couples the publish process and push notification:
- Publish runs daily at 08:00 Warsaw time
- Telegram push happens synchronously at the end of publish
- If publish takes longer than expected, the actual push time varies

**Problem:** The goal is to ensure notification and webpage update happen **exactly at a specific time** (08:00), regardless of how long the publish process takes.

**Solution:** Decouple publish and push into two independent scheduled jobs with a staging directory and retry mechanism.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SCHEDULER                                   │
│  (AsyncIOScheduler with timezone: Europe/Warsaw)                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  PUBLISH JOB (07:00 daily)                                  │   │
│  │  1. Fetch unclustered articles                              │   │
│  │  2. Event clustering + storyline resolution                  │   │
│  │  3. Summarization + freshness evaluation                    │   │
│  │  4. Render HTML/JSON to output/staging/                     │   │
│  │  5. Write .publish_complete flag file                       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ↓                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  PUSH JOB (08:00 daily)                                     │   │
│  │  1. Check .publish_complete flag                            │   │
│  │     ├─ Exists → proceed with push                          │   │
│  │     └─ Missing → schedule retry, exit                      │   │
│  │  2. Move files: staging/ → output/                         │   │
│  │  3. Update "latest" symlink                                 │   │
│  │  4. Send Telegram notification                             │   │
│  │  5. Cleanup: remove .publish_complete flag                 │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ↓                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  RETRY JOBS (conditional, up to 3 attempts)                 │   │
│  │  1. Check .publish_complete flag                            │   │
│  │     ├─ Exists → proceed with push                          │   │
│  │     └─ Missing → schedule next retry or fail               │   │
│  │  2. Same push steps as above                                │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Changes

1. **Split `Scheduler.publish()`** into:
   - `_publish_to_staging()`: Original publish logic, writes to staging
   - `_push_to_output()`: New method that moves files and sends Telegram

2. **New scheduler jobs:**
   - Publish job: 07:00 daily (changed from 08:00)
   - Push job: 08:00 daily (new)

3. **Staging directory:** `output/staging/` holds files until push

4. **Completion flag:** `.publish_complete` indicates publish finished

5. **Retry mechanism:** Up to 3 attempts with 10-minute intervals

---

## Components

### 1. Configuration Changes

**File:** `config/config.yaml`

```yaml
schedule:
  collect_cron: "0 */4 * * *"        # unchanged: every 4 hours
  publish_cron: "0 7 * * *"          # changed: now at 07:00
  push_cron: "0 8 * * *"             # new: push at 08:00
  timezone: "Europe/Warsaw"
  push_retry:
    enabled: true
    max_attempts: 3
    retry_interval_minutes: 10

output:
  html_dir: "output"
  latest_symlink: true
  staging_dir: "output/staging"      # new
  publish_complete_flag: ".publish_complete"  # new
```

### 2. Scheduler Changes

**File:** `newsprism/runtime/scheduler.py`

**New Methods:**
- `_publish_to_staging()`: Original `publish()` logic, writes to staging
- `_push_to_output()`: New method for moving files and sending Telegram
- `_check_publish_complete()`: Checks for completion flag
- `_schedule_retry(attempt)`: Schedules next retry job
- `_handle_push_failure()`: Handles final retry failure
- `_cleanup_old_staging()`: Removes stale staging files on startup

**Modified Methods:**
- `__init__()`: Parse new retry config
- `start()`: Add both publish and push cron jobs

### 3. Renderer Changes

**File:** `newsprism/runtime/renderer.py`

**Modified Method:** `HtmlRenderer.render()`
- Add `staging` parameter (bool, default false)
- When `staging=true`: write to `output/staging/`
- When `staging=false`: write to `output/` (current behavior)

### 4. Publisher Changes

**File:** `newsprism/runtime/publisher.py`

**New Method:** `TelegramPublisher.publish_with_url(base_url, staging_path)`
- Accepts staging path for generating links
- Links point to staging files until move completes

---

## Data Flow

### Normal Flow (No Retry)

```
07:00  Publish Job Starts
       ↓
       Fetch articles, cluster, summarize (existing logic)
       ↓
       renderer.render(staging=True)
       ↓
       Write files to output/staging/:
         - 2026-04-16.html
         - 2026-04-16.json
       ↓
       Write output/staging/.publish_complete
       ↓
       Publish Job Complete (duration: ~10-30 min)

08:00  Push Job Starts
       ↓
       _check_publish_complete() → True
       ↓
       Move output/staging/*.html → output/
       Move output/staging/*.json → output/
       ↓
       Update output/latest → 2026-04-16.html
       ↓
       publisher.publish_with_url(base_url, staging=False)
       ↓
       Send Telegram message with link to final URL
       ↓
       Remove output/staging/.publish_complete
       ↓
       Push Job Complete
```

### Retry Flow (Publish Delayed)

```
07:00  Publish Job Starts
       ↓
       [Processing slow due to API delays...]
       ↓
       08:00  Push Job Starts (Publish NOT done yet)
              ↓
              _check_publish_complete() → False
              ↓
              Log: "Publish not complete, scheduling retry 1/3"
              ↓
              _schedule_retry(attempt=1, delay=10min)
              ↓
              Push Job Exits (no files moved, no notification)

08:10  Retry Job 1 Starts (Publish STILL not done)
       ↓
       _check_publish_complete() → False
       ↓
       Log: "Publish not complete, scheduling retry 2/3"
       ↓
       _schedule_retry(attempt=2, delay=10min)
       ↓
       Retry Job 1 Exits

08:15  Publish Job FINALLY completes
       ↓
       Write output/staging/.publish_complete

08:20  Retry Job 2 Starts
       ↓
       _check_publish_complete() → True
       ↓
       [Normal push flow - move files, send Telegram]
       ↓
       Push Successful
```

---

## Error Handling

### Publish Phase Errors

| Error Type | Handling | State After |
|------------|----------|-------------|
| Clustering error | Log + exit | No staging files, next day's publish will retry |
| AI API error | Log + exit | No staging files, next day's publish will retry |
| Renderer error | Log + exit | No staging files, next day's publish will retry |

**No `.publish_complete` flag is written** if any step fails.

### Push Phase Errors

| Error Type | Handling | Retry Behavior |
|------------|----------|----------------|
| `.publish_complete` missing | Schedule retry, exit | Next retry in 10min |
| File move fails | Log + reschedule retry | Retry up to 3 attempts |
| Telegram API error | Log + reschedule retry | Retry up to 3 attempts |
| Max retries exceeded | Log critical + admin alert | Staging files remain for manual fix |

### Retry State Management

**APScheduler Job IDs:**
- `publish_daily` - Fixed daily at 07:00
- `push_daily` - Fixed daily at 08:00
- `push_retry_{date}_{N}` - Dynamic, N = 1,2,3

### Stale Staging Cleanup

On scheduler startup, remove staging files older than 24 hours to prevent cross-day conflicts.

---

## Testing

### Unit Tests

**File:** `tests/test_scheduler_staging.py`

- `test_publish_writes_to_staging()`
- `test_publish_creates_completion_flag()`
- `test_push_checks_completion_flag()`
- `test_push_moves_files_to_output()`
- `test_schedule_retry_on_missing_flag()`

### Integration Tests

1. **Normal Flow:** Publish → Push → Verify output
2. **Delayed Publish:** Push before publish → Retry → Success
3. **Publish Failure:** No flag created → All retries fail
4. **Stale Cleanup:** Old staging files removed on startup

### Manual Testing

```bash
# Test publish to staging
docker exec newsprism python -m newsprism publish --staging

# Test push manually
docker exec newsprism python -m newsprism push
```

---

## Implementation Plan

See implementation plan created by `writing-plans` skill.

---

## Decisions Made

1. **Approach 1 (Independent Scheduler Jobs + Staging Directory)** was chosen for simplicity and maintainability
2. **Retry mechanism** added to handle delayed publish scenarios
3. **Collection frequency** unchanged (6x daily) - to be discussed in separate session
4. **Configuration** uses separate cron schedules for publish and push
