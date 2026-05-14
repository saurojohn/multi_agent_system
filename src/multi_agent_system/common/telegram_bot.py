"""Telegram Bot integration for task submission and result retrieval."""

import logging
import threading
import time
import json
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('telegram_bot')


class BotCommand(Enum):
    """Bot command types."""
    START = "/start"
    HELP = "/help"
    STATUS = "/status"
    LIST = "/list"
    SUBMIT = "/submit"
    RESULT = "/result"
    CANCEL = "/cancel"


@dataclass
class TelegramUpdate:
    """An update from Telegram."""
    update_id: int
    message: Dict
    chat_id: int = None
    text: str = None
    username: str = None


@dataclass
class TaskSubmission:
    """A task submission via bot."""
    task_id: str
    task_type: str
    task_data: Dict
    chat_id: int
    submitted_at: float = field(default_factory=time.time)
    status: str = "pending"


class TelegramBot:
    """
    Telegram Bot for interacting with the multi-agent system.
    """

    def __init__(self, token: str = None, orchestrator=None):
        self.token = token or "YOUR_BOT_TOKEN"
        self.orchestrator = orchestrator
        self._running = False
        self._update_thread: threading.Thread = None
        self._handlers: Dict[str, Callable] = {}
        self._pending_tasks: Dict[str, TaskSubmission] = {}
        self._chat_tasks: Dict[int, List[str]] = {}  # chat_id -> task_ids
        self._lock = threading.RLock()
        self._offset = 0

    def set_orchestrator(self, orchestrator):
        """Set the orchestrator reference."""
        self.orchestrator = orchestrator

    def register_handler(self, command: str, handler: Callable):
        """Register a command handler."""
        self._handlers[command] = handler

    def start(self, polling: bool = True):
        """Start the bot."""
        self._running = True
        if polling:
            self._update_thread = threading.Thread(target=self._poll_updates, daemon=True)
            self._update_thread.start()
        logger.info("Telegram bot started")

    def stop(self):
        """Stop the bot."""
        self._running = False
        logger.info("Telegram bot stopped")

    def _poll_updates(self):
        """Poll for updates from Telegram."""
        import urllib.request
        import urllib.error

        while self._running:
            try:
                url = f"https://api.telegram.org/bot{self.token}/getUpdates?offset={self._offset}&timeout=30"
                with urllib.request.urlopen(url, timeout=35) as response:
                    data = json.loads(response.read().decode())
                    if data.get("ok"):
                        for update in data.get("result", []):
                            self._process_update(update)
                            self._offset = update["update_id"] + 1
            except Exception as e:
                logger.error(f"Polling error: {e}")
                time.sleep(5)

    def _process_update(self, update: Dict):
        """Process a single update."""
        try:
            message = update.get("message", {})
            chat = message.get("chat", {})
            text = message.get("text", "")
            chat_id = chat.get("id")
            username = chat.get("username", "unknown")

            telegram_update = TelegramUpdate(
                update_id=update["update_id"],
                message=message,
                chat_id=chat_id,
                text=text,
                username=username
            )

            self._handle_message(telegram_update)
        except Exception as e:
            logger.error(f"Error processing update: {e}")

    def _handle_message(self, update: TelegramUpdate):
        """Handle an incoming message."""
        text = update.text.strip() if update.text else ""

        if text.startswith("/"):
            parts = text.split(" ", 1)
            command = parts[0]
            args = parts[1] if len(parts) > 1 else ""

            self._execute_command(update.chat_id, command, args, update.username)
        else:
            self._send_message(update.chat_id, "Send /help for available commands")

    def _execute_command(self, chat_id: int, command: str, args: str, username: str):
        """Execute a command."""
        if command == BotCommand.START.value:
            self._send_message(chat_id, "欢迎使用多Agent系统机器人！\n输入 /help 查看命令")
        elif command == BotCommand.HELP.value:
            self._send_help(chat_id)
        elif command == BotCommand.STATUS.value:
            self._handle_status(chat_id)
        elif command == BotCommand.LIST.value:
            self._handle_list(chat_id)
        elif command == BotCommand.SUBMIT.value:
            self._handle_submit(chat_id, args)
        elif command == BotCommand.RESULT.value:
            self._handle_result(chat_id, args)
        elif command == BotCommand.CANCEL.value:
            self._handle_cancel(chat_id, args)
        else:
            self._send_message(chat_id, f"Unknown command: {command}")

    def _send_help(self, chat_id: int):
        """Send help message."""
        help_text = """
可用命令 / Available Commands:

/start - 开始使用 / Start
/help - 显示此帮助 / Show this help
/status - 系统状态 / System status
/list - 我的任务列表 / My task list
/submit <type> <data> - 提交任务 / Submit task
/result <task_id> - 查看结果 / Get result
/cancel <task_id> - 取消任务 / Cancel task

示例 / Examples:
/submit analysis {"query": "sales data"}
/result abc123
"""
        self._send_message(chat_id, help_text)

    def _handle_status(self, chat_id: int):
        """Handle status command."""
        if not self.orchestrator:
            self._send_message(chat_id, "Orchestrator未设置 / Orchestrator not set")
            return

        try:
            workers = self.orchestrator.get_workers_status()
            tasks = self.orchestrator.get_all_tasks()

            status_text = f"""
系统状态 / System Status:

Workers: {len(workers)}
- 在线: {sum(1 for w in workers if w.get('status') == 'online')}
- 离线: {sum(1 for w in workers if w.get('status') != 'online')}

Tasks: {len(tasks)}
- 待处理: {sum(1 for t in tasks if t.get('status') == 'pending')}
- 进行中: {sum(1 for t in tasks if t.get('status') == 'running')}
- 完成: {sum(1 for t in tasks if t.get('status') == 'completed')}
"""
            self._send_message(chat_id, status_text)
        except Exception as e:
            self._send_message(chat_id, f"Error: {e}")

    def _handle_list(self, chat_id: int):
        """Handle list command."""
        with self._lock:
            task_ids = self._chat_tasks.get(chat_id, [])

        if not task_ids:
            self._send_message(chat_id, "暂无任务 / No tasks")
            return

        lines = ["你的任务 / Your tasks:\n"]
        for task_id in task_ids[-10:]:  # Last 10
            if task_id in self._pending_tasks:
                task = self._pending_tasks[task_id]
                lines.append(f"• {task_id[:8]}... - {task.task_type} ({task.status})")

        self._send_message(chat_id, "\n".join(lines))

    def _handle_submit(self, chat_id: int, args: str):
        """Handle submit command."""
        if not self.orchestrator:
            self._send_message(chat_id, "Orchestrator未设置 / Orchestrator not set")
            return

        try:
            parts = args.split(" ", 1)
            if len(parts) < 2:
                self._send_message(chat_id, "用法: /submit <type> <data>\nExample: /submit analysis {'query': 'sales'}")
                return

            task_type = parts[0]
            try:
                task_data = json.loads(parts[1]) if parts[1] else {}
            except:
                task_data = {"raw": parts[1]}

            task_id = self.orchestrator.submit_task(task_type, task_data)

            submission = TaskSubmission(
                task_id=task_id,
                task_type=task_type,
                task_data=task_data,
                chat_id=chat_id
            )

            with self._lock:
                self._pending_tasks[task_id] = submission
                if chat_id not in self._chat_tasks:
                    self._chat_tasks[chat_id] = []
                self._chat_tasks[chat_id].append(task_id)

            self._send_message(chat_id, f"任务已提交 / Task submitted\nID: {task_id[:8]}...\n类型: {task_type}")

        except Exception as e:
            self._send_message(chat_id, f"提交失败 / Submit failed: {e}")

    def _handle_result(self, chat_id: int, args: str):
        """Handle result command."""
        if not self.orchestrator:
            self._send_message(chat_id, "Orchestrator未设置 / Orchestrator not set")
            return

        if not args:
            self._send_message(chat_id, "用法: /result <task_id>")
            return

        task_id = args.strip()
        try:
            status = self.orchestrator.get_task_status(task_id)
            if status["status"] == "completed":
                result = status.get("result", {})
                self._send_message(chat_id, f"结果 / Result:\n{json.dumps(result, indent=2, ensure_ascii=False)}")
            elif status["status"] == "failed":
                self._send_message(chat_id, f"任务失败 / Task failed: {status.get('error')}")
            else:
                self._send_message(chat_id, f"状态: {status['status']}")
        except Exception as e:
            self._send_message(chat_id, f"Error: {e}")

    def _handle_cancel(self, chat_id: int, args: str):
        """Handle cancel command."""
        if not args:
            self._send_message(chat_id, "用法: /cancel <task_id>")
            return

        task_id = args.strip()
        with self._lock:
            if task_id in self._pending_tasks:
                submission = self._pending_tasks[task_id]
                if submission.chat_id == chat_id:
                    # Mark as cancelled (actual cancellation depends on orchestrator support)
                    submission.status = "cancelled"
                    self._send_message(chat_id, f"任务已取消 / Task cancelled: {task_id[:8]}...")
                else:
                    self._send_message(chat_id, "不是你的任务 / Not your task")
            else:
                self._send_message(chat_id, "任务不存在 / Task not found")

    def _send_message(self, chat_id: int, text: str):
        """Send a message via Telegram."""
        import urllib.request
        import urllib.error

        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            data = json.dumps({
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown"
            }).encode()

            req = urllib.request.Request(url, data=data)
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=10):
                pass
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    def send_notification(self, chat_id: int, message: str):
        """Send a notification to a user."""
        self._send_message(chat_id, message)

    def broadcast_to_tasks(self, message: str, task_type: str = None):
        """Broadcast message to all users with tasks of a certain type."""
        with self._lock:
            for task_id, submission in self._pending_tasks.items():
                if task_type is None or submission.task_type == task_type:
                    self._send_message(submission.chat_id, message)

    def notify_task_complete(self, task_id: str, result: Dict):
        """Notify user when their task is complete."""
        with self._lock:
            if task_id in self._pending_tasks:
                submission = self._pending_tasks[task_id]
                self._send_message(
                    submission.chat_id,
                    f"✅ 任务完成 / Task Complete\n"
                    f"ID: {task_id[:8]}...\n"
                    f"结果: {json.dumps(result, ensure_ascii=False)[:200]}..."
                )


class TelegramBotMiddleware:
    """Middleware to integrate Telegram bot with orchestrator."""

    def __init__(self, bot: TelegramBot, orchestrator):
        self.bot = bot
        self.orchestrator = orchestrator
        bot.set_orchestrator(orchestrator)

    def start(self):
        """Start the middleware."""
        self.bot.start()

    def stop(self):
        """Stop the middleware."""
        self.bot.stop()


# Global bot instance
_bot = TelegramBot()


def get_bot() -> TelegramBot:
    """Get global bot instance."""
    return _bot


def configure_bot(token: str, orchestrator=None) -> TelegramBot:
    """Configure the bot with token and orchestrator."""
    _bot.token = token
    if orchestrator:
        _bot.set_orchestrator(orchestrator)
    return _bot


def start_bot(polling: bool = True):
    """Start the bot."""
    _bot.start(polling)


def stop_bot():
    """Stop the bot."""
    _bot.stop()