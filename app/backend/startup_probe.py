import os
import sys
import time
import traceback
from pathlib import Path

LOG_PATHS = [Path('/tmp/startup_error.log')]
if os.path.isdir('/data'):
    LOG_PATHS.append(Path('/data/startup_error.log'))


def write_log(text: str):
    for path in LOG_PATHS:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('a', encoding='utf-8') as f:
                f.write(text)
                if not text.endswith('\n'):
                    f.write('\n')
        except Exception:
            pass
    try:
        sys.stderr.write(text)
        if not text.endswith('\n'):
            sys.stderr.write('\n')
        sys.stderr.flush()
    except Exception:
        pass


def main():
    write_log('=== startup probe begin ===')
    write_log(f'python={sys.version}')
    write_log(f'cwd={os.getcwd()}')
    write_log(f'PORT={os.getenv("PORT")} WEB_PORT={os.getenv("WEB_PORT")}')
    write_log(f'DATABASE_URL={os.getenv("DATABASE_URL")}')
    port_value = os.getenv('WEB_PORT') or os.getenv('PORT') or '8000'
    if not str(port_value).isdigit():
        port_value = '8000'
    try:
        import uvicorn
        from app.main import app
        write_log('app import ok')
        uvicorn.run(app, host='0.0.0.0', port=int(port_value))
    except Exception:
        write_log('startup exception:')
        write_log(traceback.format_exc())
        # keep container alive for inspection instead of immediate crash loop
        time.sleep(600)
        raise


if __name__ == '__main__':
    main()
