# -*- coding: utf-8 -*-
"""按级别和原因聚合 warning/error 日志。

StatLogger 只面向 Prefect flow/task 运行上下文使用。
"""

import threading
from collections import Counter


def get_stat_logger(module_name: str = "stat_logger") -> "StatLogger":
    """获取统计日志器。必须在 Prefect flow/task 运行上下文中调用。"""
    return ensure_stat_logger(module_name=module_name)


def ensure_stat_logger(logger=None, module_name: str = "stat_logger") -> "StatLogger":
    """确保传入对象是 StatLogger，并校验当前处于 Prefect run 上下文。"""
    prefect_logger = _get_base_logger(module_name)
    if isinstance(logger, StatLogger):
        return logger
    if logger is None:
        logger = prefect_logger
    return StatLogger(logger)


def _get_base_logger(module_name: str):
    try:
        from prefect import get_run_logger

        return get_run_logger()
    except Exception as exc:
        raise RuntimeError(
            f"StatLogger 只能在 Prefect flow/task 运行上下文中使用: {module_name}"
        ) from exc


class StatLogger:
    """轻量日志统计器。

    reason 使用原日志的固定文本；detail 放账号、代码、异常等动态值。
    """

    def __init__(self, logger):
        self.logger = logger
        self.groups = {"error": {}, "warning": {}}
        self._lock = threading.Lock()

    def debug(self, message: str, *args, **kwargs) -> None:
        self.logger.debug(message, *args, **kwargs)

    def info(self, message: str, *args, **kwargs) -> None:
        self.logger.info(message, *args, **kwargs)

    def exception(self, message: str, *args, **kwargs) -> None:
        if hasattr(self.logger, "exception"):
            self.logger.exception(message, *args, **kwargs)
        else:
            self.logger.error(message, *args, **kwargs)

    def __getattr__(self, name: str):
        return getattr(self.logger, name)

    def error(self, reason: str, detail=None) -> None:
        self._add("error", reason, detail)

    def warning(self, reason: str, detail=None) -> None:
        self._add("warning", reason, detail)

    def error_now(self, message: str, *args, **kwargs) -> None:
        self.logger.error(message, *args, **kwargs)

    def warning_now(self, message: str, *args, **kwargs) -> None:
        self.logger.warning(message, *args, **kwargs)

    def _add(self, level: str, reason: str, detail=None) -> None:
        with self._lock:
            item = self.groups[level].setdefault(
                self._one_line(reason),
                {"count": 0, "details": Counter()},
            )
            item["count"] += 1
            if detail is not None:
                item["details"][self._one_line(detail)] += 1

    def flush(self) -> None:
        with self._lock:
            groups = self.groups
            self.groups = {"error": {}, "warning": {}}

        for level, reasons in groups.items():
            if not reasons:
                continue
            log = getattr(self.logger, level)
            for reason, item in reasons.items():
                log(self._format_reason(reason, item))

    def level_count(self, level: str) -> int:
        """返回指定级别已聚合日志条数。"""
        with self._lock:
            return sum(item["count"] for item in self.groups.get(level, {}).values())

    def error_count(self) -> int:
        """返回 error 级别已聚合日志条数。"""
        return self.level_count("error")

    def level_summary(
        self,
        level: str,
        *,
        max_reasons: int = 8,
        max_details_per_reason: int = 5,
    ) -> str:
        """返回指定级别聚合日志摘要，不清空已有统计。"""
        with self._lock:
            reasons = [
                (
                    reason,
                    {
                        "count": item["count"],
                        "details": item["details"].copy(),
                    },
                )
                for reason, item in self.groups.get(level, {}).items()
            ]

        if not reasons:
            return ""

        parts = []
        for reason, item in reasons[:max_reasons]:
            parts.append(self._format_limited_reason(reason, item, max_details_per_reason))

        hidden = len(reasons) - max_reasons
        if hidden > 0:
            parts.append(f"另有 {hidden} 类{level}日志")
        return "；".join(parts)

    def error_summary(
        self,
        *,
        max_reasons: int = 8,
        max_details_per_reason: int = 5,
    ) -> str:
        """返回 error 级别聚合日志摘要。"""
        return self.level_summary(
            "error",
            max_reasons=max_reasons,
            max_details_per_reason=max_details_per_reason,
        )

    def clear(self) -> None:
        with self._lock:
            self.groups = {"error": {}, "warning": {}}

    def _format_reason(self, reason: str, item: dict) -> str:
        details = item["details"]
        if not details:
            return f"{reason}({item['count']})"

        return f"{reason}({item['count']}): {'；'.join(details)}"

    def _format_limited_reason(
        self,
        reason: str,
        item: dict,
        max_details_per_reason: int,
    ) -> str:
        details = item["details"]
        if not details:
            return f"{reason}({item['count']})"

        detail_items = [
            detail
            for detail, _ in details.most_common(max_details_per_reason)
        ]
        hidden = len(details) - max_details_per_reason
        if hidden > 0:
            detail_items.append(f"另有 {hidden} 项")
        return f"{reason}({item['count']}): {'；'.join(detail_items)}"

    @staticmethod
    def _one_line(value) -> str:
        return " ".join(str(value).splitlines())
