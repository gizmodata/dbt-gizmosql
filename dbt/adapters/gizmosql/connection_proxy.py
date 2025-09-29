from typing import Any, Callable, Optional
from weakref import WeakSet

def wrap_with_autoclosing_cursors(conn: Any) -> Any:
    """
    Wrap a DB-API/ADBC connection so that:
      - every .cursor() is tracked
      - connection.close()/commit()/rollback() first close all open cursors
      - cursors still auto-close via finalizer/ __del__ as a fallback
    Idempotent: calling twice returns the existing wrapper.
    """
    if getattr(conn, "_gizmo_autoclose_wrapped", False):
        return conn  # already wrapped

    def _safe_call(fn: Optional[Callable[[], None]]):
        try:
            if callable(fn):
                fn()
        except Exception:
            pass

    class _AutoClosingCursor:
        __slots__ = ("_inner", "_owner_conn", "__weakref__")

        def __init__(self, inner, owner_conn):
            self._inner = inner
            self._owner_conn = owner_conn

        # Ensure explicit close removes from the connection’s tracking set
        def close(self):
            try:
                self._owner_conn._cursors.discard(self)  # type: ignore[attr-defined]
            except Exception:
                pass
            _safe_call(getattr(self._inner, "close", None))

        # Fallback: if GC runs later, still attempt to close
        def __del__(self):
            try:
                self.close()
            except Exception:
                pass

        # Delegate everything else to the real cursor
        def __getattr__(self, name):
            return getattr(self._inner, name)

        # Preserve context manager behavior
        def __enter__(self):
            enter = getattr(self._inner, "__enter__", None)
            return enter() if callable(enter) else self

        def __exit__(self, exc_type, exc, tb):
            exit_ = getattr(self._inner, "__exit__", None)
            if callable(exit_):
                return exit_(exc_type, exc, tb)
            self.close()
            return False

        def __iter__(self):
            return iter(self._inner)

    class _ConnProxy:
        __slots__ = ("_inner", "_cursors", "_gizmo_autoclose_wrapped", "__weakref__")

        def __init__(self, inner):
            self._inner = inner
            self._cursors: "WeakSet[_AutoClosingCursor]" = WeakSet()
            self._gizmo_autoclose_wrapped = True

        def _close_all_cursors(self):
            # Copy to a list to avoid mutating while iterating
            for cur in list(self._cursors):
                try:
                    cur.close()
                except Exception:
                    pass
            self._cursors.clear()

        def cursor(self, *args, **kwargs):
            real = self._inner.cursor(*args, **kwargs)
            wrapped = _AutoClosingCursor(real, self)
            self._cursors.add(wrapped)
            return wrapped

        # Ensure close/commit/rollback clean up first
        def close(self):
            self._close_all_cursors()
            _safe_call(getattr(self._inner, "close", None))

        def commit(self):
            _safe_call(getattr(self._inner, "commit", None))

        def rollback(self):
            self._close_all_cursors()
            _safe_call(getattr(self._inner, "rollback", None))

        # Delegate the rest
        def __getattr__(self, name):
            return getattr(self._inner, name)

        def __enter__(self):
            enter = getattr(self._inner, "__enter__", None)
            return _ConnProxy(enter()) if callable(enter) else self

        def __exit__(self, exc_type, exc, tb):
            exit_ = getattr(self._inner, "__exit__", None)
            if callable(exit_):
                return exit_(exc_type, exc, tb)
            # mimic context manager closing semantics
            self.close()
            return False

    return _ConnProxy(conn)
