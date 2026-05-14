#!/usr/bin/env python3
"""Example: Telegram Bot for task submission and result retrieval."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator
from multi_agent_system.common.telegram_bot import TelegramBot, configure_bot, start_bot, stop_bot


def main():
    # Initialize message queue
    mq = MessageQueueManager()

    # Create and start orchestrator
    orch = Orchestrator(mq)
    orch.start()

    # Configure Telegram bot with your token
    # Get token from @BotFather on Telegram
    BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"

    bot = configure_bot(BOT_TOKEN, orch)

    print("Starting Telegram Bot...")
    print("Send /start to your bot on Telegram")
    print("Press Ctrl+C to stop")

    # Start bot polling
    start_bot(polling=True)

    try:
        # Keep running
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping bot...")
        stop_bot()
        orch.stop()
        print("Done")


if __name__ == "__main__":
    main()