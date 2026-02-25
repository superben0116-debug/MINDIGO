from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
import os
import threading
import time
from app.db import Base, engine
from app.db import SessionLocal
from app import models, crud
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from app.auth import get_current_user_optional, ensure_default_admin
from app.services import execute_sync_job
from app.interface_registry import default_interface_registry
from app.config_store import default_kapi_config, default_lingxing_config, default_shipper_config
from app.routers.internal_orders import router as internal_orders_router
from app.routers.supplier_quotes import router as supplier_quotes_router
from app.routers.kapi_exports import router as kapi_exports_router
from app.routers.lingxing import router as lingxing_router
from app.routers.config import router as config_router
from app.routers.dev import router as dev_router
from app.routers.lingxing_tools import router as lingxing_tools_router
from app.routers.import_jobs import router as import_jobs_router
from app.routers.auth import router as auth_router

app = FastAPI(title="Ultimate ERP")
_auto_sync_started = False


def _is_truthy(v: str | None) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "on")


def _looks_placeholder(v: str | None) -> bool:
    s = str(v or "").strip().lower()
    return s in ("", "app_id", "access_token", "sid1")


def _config_ready_for_sync(cfg: dict) -> bool:
    app_id = str(cfg.get("app_id") or "").strip()
    app_secret = str(cfg.get("app_secret") or "").strip()
    sid_list = str(cfg.get("sid_list") or "").strip()
    if _looks_placeholder(app_id) or _looks_placeholder(app_secret):
        return False
    return bool(app_id and app_secret and sid_list)


def _auto_sync_loop():
    interval_min = int(os.getenv("ERP_AUTO_SYNC_INTERVAL_MINUTES", "30") or "30")
    if interval_min < 5:
        interval_min = 5
    while True:
        db = SessionLocal()
        try:
            cfg = crud.get_config(db, "lingxing")
            lxcfg = cfg.config_value if cfg and isinstance(cfg.config_value, dict) else {}
            if _config_ready_for_sync(lxcfg):
                running = (
                    db.query(models.ImportJob.id)
                    .filter(
                        models.ImportJob.job_type == "lingxing_fbm",
                        models.ImportJob.status.in_(["queued", "running"]),
                    )
                    .first()
                )
                if not running:
                    job = crud.create_import_job(db, "lingxing_fbm")
                    execute_sync_job(job.id)
        except Exception:
            pass
        finally:
            db.close()
        time.sleep(interval_min * 60)


class UINoCacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        # UI access control
        if path.startswith("/ui"):
            public_ui = {"/ui/index.html", "/ui/admin_login.html", "/ui/supplier_login.html"}
            if path not in public_ui:
                db = SessionLocal()
                try:
                    user = get_current_user_optional(request, db)
                finally:
                    db.close()
                if not user:
                    target = "/ui/supplier_login.html" if path.startswith("/ui/supplier_") else "/ui/admin_login.html"
                    return RedirectResponse(url=target, status_code=307)
                if user.role == "supplier" and path not in {"/ui/supplier_quote.html", "/ui/supplier_login.html"}:
                    return RedirectResponse(url="/ui/supplier_quote.html", status_code=307)
        # API access control
        protected_admin_prefixes = (
            "/internal-orders",
            "/kapi-exports",
            "/config",
            "/integrations/lingxing",
            "/import-jobs",
            "/dev",
        )
        protected_supplier_prefixes = ("/supplier-quotes",)
        if path.startswith(protected_admin_prefixes) or path.startswith(protected_supplier_prefixes):
            if not path.startswith("/auth"):
                db = SessionLocal()
                try:
                    user = get_current_user_optional(request, db)
                finally:
                    db.close()
                if not user:
                    return JSONResponse(status_code=401, content={"error": "not_authenticated"})
                if path.startswith(protected_admin_prefixes) and user.role != "admin":
                    return JSONResponse(status_code=403, content={"error": "admin_required"})
        resp = await call_next(request)
        if path.startswith("/ui"):
            resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            resp.headers["Pragma"] = "no-cache"
            resp.headers["Expires"] = "0"
        return resp


app.add_middleware(UINoCacheMiddleware)

app.include_router(lingxing_router, prefix="/integrations/lingxing", tags=["lingxing"])
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(internal_orders_router, prefix="/internal-orders", tags=["internal-orders"])
app.include_router(supplier_quotes_router, prefix="/supplier-quotes", tags=["supplier-quotes"])
app.include_router(kapi_exports_router, prefix="/kapi-exports", tags=["kapi-exports"])
app.include_router(config_router, prefix="/config", tags=["config"])
app.include_router(dev_router, prefix="/dev", tags=["dev"])
app.include_router(lingxing_tools_router, prefix="/integrations/lingxing", tags=["lingxing-tools"])
app.include_router(import_jobs_router, prefix="/import-jobs", tags=["import-jobs"])

frontend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "frontend"))
app.mount("/ui", StaticFiles(directory=frontend_dir, html=True), name="ui")

@app.on_event("startup")
def startup():
    # Ensure tables exist for local testing (SQLite default)
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        ensure_default_admin(db)
        existing_lx = crud.get_config(db, "lingxing")
        lx_defaults = default_lingxing_config()
        if not existing_lx:
            crud.set_config(db, "lingxing", lx_defaults)
        else:
            cur = existing_lx.config_value if isinstance(existing_lx.config_value, dict) else {}
            # Auto bootstrap from ENV when current value is empty/placeholder.
            changed = False
            for k in ("app_id", "app_secret", "access_token", "sid_list"):
                cv = str(cur.get(k) or "").strip()
                dv = str(lx_defaults.get(k) or "").strip()
                if (_looks_placeholder(cv) or not cv) and dv and not _looks_placeholder(dv):
                    cur[k] = dv
                    changed = True
            if changed:
                crud.set_config(db, "lingxing", cur)
        if not crud.get_config(db, "shipper"):
            crud.set_config(db, "shipper", default_shipper_config())
        if not crud.get_config(db, "kapi"):
            crud.set_config(db, "kapi", default_kapi_config())
        if not crud.get_config(db, "interface_registry"):
            crud.set_config(db, "interface_registry", default_interface_registry())
    finally:
        db.close()
    global _auto_sync_started
    if _auto_sync_started:
        return
    if _is_truthy(os.getenv("ERP_AUTO_SYNC_ENABLED", "1")):
        t = threading.Thread(target=_auto_sync_loop, daemon=True)
        t.start()
        _auto_sync_started = True

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"error": "internal_error", "detail": str(exc)},
    )

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def root():
    return RedirectResponse(url="/ui/index.html", status_code=307)
