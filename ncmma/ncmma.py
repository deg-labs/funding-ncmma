#!/usr/bin/env python3
"""
CMMA API Funding Rate 監視バッチシステム
- 異常なFunding Rateを検知し、その継続性を分析して通知
- 重複投稿防止機能 (SQLite)
- ログファイル自動削除機能
"""

import hashlib
import logging
import math
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import requests
from dotenv import load_dotenv


class CmmaFundingRateMonitor:
    def __init__(self, config_path=None):
        # スクリプトのディレクトリを基準にパスを設定
        self.script_dir = Path(__file__).parent.absolute()
        self.config_path = config_path or self.script_dir / ".env"
        self.log_dir = self.script_dir / "logs"
        self.cache_dir = self.script_dir / "cache"

        # ディレクトリの作成
        self.log_dir.mkdir(exist_ok=True)
        self.cache_dir.mkdir(exist_ok=True)

        # 設定の読み込みとロギング設定
        self._load_config()
        self._setup_logging()

        # データベースの初期化
        self.data_dir = self.script_dir / "data"
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / "ncmma.db"
        self._init_db()

        if not self.discord_webhook_url:
            self.logger.warning(
                "DISCORD_WEBHOOK_URL not found in environment variables"
            )

    def _load_config(self):
        """設定ファイルの読み込み"""
        if self.config_path.exists():
            load_dotenv(self.config_path)
        else:
            load_dotenv()

        self.discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

        # API設定
        # デフォルトはユーザー指定のベースURL + エンドポイントパス
        self.api_base_url = os.getenv("CMMA_API_BASE_URL", "https://stg.api.1btc.love")

        # Funding Rate 監視設定
        self.fr_threshold = float(
            os.getenv("FUNDING_RATE_THRESHOLD", "0.001")
        )  # デフォルト 0.1%
        self.fr_direction = os.getenv(
            "FUNDING_RATE_DIRECTION", "both"
        )  # positive, negative, both
        self.fr_sort = os.getenv("FUNDING_RATE_SORT", "funding_abs_desc")
        self.fr_limit = int(os.getenv("FUNDING_RATE_LIMIT", "100"))

        # 継続性分析設定
        self.lookback = int(os.getenv("FUNDING_RATE_LOOKBACK", "24"))

        # 監視設定
        self.max_notifications = int(os.getenv("MAX_NOTIFICATIONS", "20"))
        self.renotify_buffer_minutes = int(
            os.getenv("RENOTIFY_BUFFER_MINUTES", "240")
        )  # デフォルト4時間
        self.check_interval_seconds = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))

        # Discordメッセージ保持設定
        self.discord_keep_messages = int(os.getenv("DISCORD_KEEP_MESSAGES", "50"))
        self.discord_cleanup_interval_hours = int(
            os.getenv("DISCORD_CLEANUP_INTERVAL_HOURS", "24")
        )
        self.discord_delete_interval_seconds = float(
            os.getenv("DISCORD_DELETE_INTERVAL_SECONDS", "0.5")
        )

        # ログ設定
        self.log_max_size_mb = int(os.getenv("LOG_MAX_SIZE_MB", "10"))

    def _init_db(self):
        """データベースの初期化"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # テーブル定義を更新 (volatility用からfunding rate用に変更)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS funding_notifications (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        notification_hash TEXT UNIQUE NOT NULL,
                        symbol TEXT NOT NULL,
                        rate REAL NOT NULL,
                        direction TEXT NOT NULL,
                        notified_at TIMESTAMP NOT NULL
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS discord_messages (
                        message_id TEXT PRIMARY KEY,
                        posted_at TIMESTAMP NOT NULL
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """)
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise

    def _setup_logging(self):
        """ログ設定"""
        log_file = self.log_dir / "ncmma_monitor.log"

        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

        # ファイルハンドラー（ローテーション付き）
        # ログサイズ設定: 指定サイズをバックアップ数+1で割って、合計が指定サイズに収まるようにする
        # 例: 10MB, backup=3 -> 1ファイルあたり2.5MB
        backup_count = 3
        max_bytes = int((self.log_max_size_mb * 1024 * 1024) / (backup_count + 1))

        file_handler = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)

        # コンソールハンドラー
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)

        # ロガー設定
        self.logger = logging.getLogger("CmmaFundingRateMonitor")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

    def _generate_notification_hash(self, symbol, direction):
        """通知用ハッシュを生成"""
        # シンボル + 方向 + (日付や時間帯を含めるか検討したが、
        # 指定時間内(RENOTIFY_BUFFER_MINUTES)の重複を防ぐ目的なのでシンプルに)
        hash_input = f"{symbol}_{direction}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _should_notify(self, notification_hash):
        """通知すべきかどうかをDBで判定"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT notified_at FROM funding_notifications WHERE notification_hash = ?",
                    (notification_hash,),
                )
                result = cursor.fetchone()

                if result:
                    last_notified_at = datetime.fromisoformat(result[0])
                    time_diff = datetime.now() - last_notified_at

                    if time_diff.total_seconds() < self.renotify_buffer_minutes * 60:
                        return False
                return True
        except (sqlite3.Error, ValueError, TypeError) as e:
            self.logger.error(f"Failed to check notification history from DB: {e}")
            return True  # DBエラー時は通知を許可し、金融アラートの喪失を防ぐ

    def _record_notification(self, notification_hash, symbol, rate, direction):
        """通知履歴をDBに記録"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO funding_notifications (notification_hash, symbol, rate, direction, notified_at)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        notification_hash,
                        symbol,
                        rate,
                        direction,
                        datetime.now().isoformat(),
                    ),
                )
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Failed to record notification to DB: {e}")

    def _discord_post_url(self):
        """wait=true を付けた投稿用URLを返す (レスポンスからメッセージIDを取得するために必要)"""
        base, sep, query = self.discord_webhook_url.partition("?")
        if sep:
            return f"{base}?{query}&wait=true"
        return f"{base}?wait=true"

    def _discord_delete_url(self, message_id):
        """webhookが投稿したメッセージを削除するURLを返す"""
        base, sep, query = self.discord_webhook_url.partition("?")
        url = f"{base}/messages/{message_id}"
        return f"{url}?{query}" if sep else url

    def _record_discord_message(self, message_id):
        """投稿したDiscordメッセージのIDをDBに記録"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO discord_messages (message_id, posted_at) VALUES (?, ?)",
                    (str(message_id), datetime.now().isoformat()),
                )
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Failed to record Discord message ID: {e}")

    def _get_meta(self, key):
        """metaテーブルから値を取得"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT value FROM meta WHERE key = ?", (key,))
                result = cursor.fetchone()
                return result[0] if result else None
        except sqlite3.Error:
            return None

    def _set_meta(self, key, value):
        """metaテーブルに値を記録"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                    (key, value),
                )
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Failed to write meta '{key}': {e}")

    def _delete_discord_message(self, message_id):
        """webhook経由で古いDiscordメッセージを削除する。成功時True"""
        for _ in range(3):
            try:
                response = requests.delete(
                    self._discord_delete_url(message_id), timeout=30
                )
                if response.status_code == 429:
                    try:
                        retry_after = float(response.json().get("retry_after", "1"))
                    except ValueError:
                        retry_after = 1.0
                    self.logger.warning(
                        f"Rate limited while deleting message {message_id}, waiting {retry_after}s"
                    )
                    time.sleep(retry_after)
                    continue
                if response.status_code == 404:
                    return True
                response.raise_for_status()
                return True
            except requests.exceptions.RequestException as e:
                self.logger.warning(
                    f"Failed to delete Discord message {message_id}: {e}"
                )
                time.sleep(1)
        return False

    def _cleanup_old_discord_messages(self, force=False):
        """保持件数を超えた古いDiscordメッセージをwebhook経由で削除する。"""
        if not self.discord_webhook_url:
            return

        last_cleanup = self._get_meta("last_discord_cleanup")
        if not force and last_cleanup:
            try:
                elapsed_hours = (
                    datetime.now() - datetime.fromisoformat(last_cleanup)
                ).total_seconds() / 3600
                if elapsed_hours < self.discord_cleanup_interval_hours:
                    return
            except ValueError:
                pass

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT message_id FROM discord_messages
                    ORDER BY posted_at DESC, rowid DESC
                    LIMIT -1 OFFSET ?
                    """,
                    (max(self.discord_keep_messages, 0),),
                )
                stale_ids = [row[0] for row in cursor.fetchall()]

            if not stale_ids:
                self.logger.info(
                    f"Discord message retention check: within limit ({self.discord_keep_messages})"
                )
                self._set_meta("last_discord_cleanup", datetime.now().isoformat())
                return

            self.logger.info(
                f"Deleting {len(stale_ids)} old Discord messages (keep={self.discord_keep_messages})"
            )
            deleted = 0
            for message_id in stale_ids:
                if self._delete_discord_message(message_id):
                    deleted += 1
                    with sqlite3.connect(self.db_path) as conn:
                        conn.execute(
                            "DELETE FROM discord_messages WHERE message_id = ?",
                            (message_id,),
                        )
                time.sleep(self.discord_delete_interval_seconds)

            self.logger.info(f"Deleted {deleted}/{len(stale_ids)} old Discord messages")
            self._set_meta("last_discord_cleanup", datetime.now().isoformat())
        except sqlite3.Error as e:
            self.logger.error(f"Database error during Discord message cleanup: {e}")

    def fetch_abnormal_funding_rates(self):
        """CMMA APIから異常なFunding Rateデータを取得"""
        url = f"{self.api_base_url}/funding-rates"
        params = {
            "threshold": self.fr_threshold,
            "direction": self.fr_direction,
            "sort": self.fr_sort,
            "limit": self.fr_limit,
        }
        try:
            self.logger.info(f"Fetching funding rates from {url} with params: {params}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if not isinstance(data, dict):
                self.logger.warning(
                    f"Unexpected response format (not an object): {data!r}"
                )
                return []
            items = data.get("data")
            if not isinstance(items, list):
                self.logger.warning(
                    f"Unexpected response format ('data' is not a list): {data!r}"
                )
                return []
            self.logger.info(f"Fetched {len(items)} abnormal funding rates.")
            return items

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error while fetching funding rates: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return []

    def fetch_continuity_stats(self, symbol, direction):
        """指定された銘柄のFunding Rate継続性統計を取得"""
        url = f"{self.api_base_url}/funding-rates/extreme-continuity"

        # directionが 'both' の場合、個別の銘柄のレートの正負に合わせてdirectionを指定する方が正確な場合があるが、
        # API仕様ではbothも可。ここでは念のため、レートの正負に合わせた方向を指定して精度を高める。
        # ただし、呼び出し元で渡されたdirectionをそのまま使う方針とする。

        params = {
            "symbol": symbol,
            "threshold": self.fr_threshold,
            "lookback": self.lookback,
            "direction": direction if direction != "both" else "both",
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"Failed to fetch continuity stats for {symbol}: {e}")
            return None

    def send_discord_notification(self, candidates):
        """Discordに通知を送信"""
        if not candidates or not self.discord_webhook_url:
            return False

        # 通知対象をフィルタリング (外部API由来データのため、構造を検証して不正要素はスキップ)
        notifications = []
        for item in candidates:
            if not isinstance(item, dict):
                self.logger.warning(
                    f"Skipping invalid candidate (not an object): {item!r}"
                )
                continue

            symbol = item.get("symbol")
            funding = item.get("funding")
            if not isinstance(symbol, str) or not isinstance(funding, dict):
                self.logger.warning(
                    f"Skipping invalid candidate (missing/invalid symbol or funding): {item!r}"
                )
                continue

            rate = funding.get("rate")
            if isinstance(rate, bool) or not isinstance(rate, (int, float)):
                self.logger.warning(
                    f"Skipping invalid candidate (missing/invalid funding rate): {item!r}"
                )
                continue

            try:
                rate_value = float(rate)
            except (OverflowError, TypeError, ValueError):
                self.logger.warning(
                    f"Skipping invalid candidate (unconvertible funding rate): {item!r}"
                )
                continue
            if not math.isfinite(rate_value):
                self.logger.warning(
                    f"Skipping invalid candidate (non-finite funding rate): {item!r}"
                )
                continue

            # レートの正負から方向を判定
            item_direction = "positive" if rate_value > 0 else "negative"

            # フィルタリング
            notif_hash = self._generate_notification_hash(symbol, item_direction)
            if self._should_notify(notif_hash):
                # 継続性統計を取得
                continuity_data = self.fetch_continuity_stats(symbol, item_direction)
                notifications.append(
                    {
                        "item": item,
                        "hash": notif_hash,
                        "direction": item_direction,
                        "stats": (
                            continuity_data.get("stats")
                            if isinstance(continuity_data, dict)
                            else None
                        ),
                    }
                )

        if not notifications:
            self.logger.info("All candidates filtered out (recently notified).")
            return False

        # 最大通知数制限
        notifications = notifications[: self.max_notifications]

        embed = {
            "title": "💰 異常金利・継続性アラート",
            "description": f"閾値 `|{self.fr_threshold:.3%}|` を超えるFunding Rateを検知しました。\n(直近のFR履歴: {self.lookback})",
            "color": 0xFF8C00,  # DarkOrange
            "fields": [],
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {"text": "CMMA Funding Rate Monitor"},
        }

        for n in notifications:
            item = n["item"]
            stats = n["stats"]
            rate = item["funding"]["rate"]
            rate_percent = f"{rate:+.4%}"
            symbol = item["symbol"]

            # Extract additional info
            constraints = item.get("constraints", {})
            interval_hours = constraints.get("interval_hours")
            next_funding_ts = item.get("next_funding_ts")

            icon = "📈" if rate > 0 else "📉"

            value_text = f"**Rate: `{rate_percent}`**"

            # Interval
            if interval_hours:
                value_text += f" (Intv: `{interval_hours}h`)"

            # Next Funding Time (JST) & Countdown
            if next_funding_ts:
                try:
                    ts_sec = next_funding_ts / 1000
                    # UTC datetime
                    dt_utc = datetime.utcfromtimestamp(ts_sec)
                    # JST datetime (UTC+9)
                    dt_jst = dt_utc + timedelta(hours=9)
                    jst_str = dt_jst.strftime("%H:%M")

                    # Countdown
                    now_utc = datetime.utcnow()
                    diff = dt_utc - now_utc
                    if diff.total_seconds() > 0:
                        hours = int(diff.total_seconds() // 3600)
                        minutes = int((diff.total_seconds() % 3600) // 60)
                        countdown = f"{hours}h{minutes:02d}m"
                    else:
                        countdown = "Now"

                    value_text += f"\nNext: `{jst_str} JST` (あと `{countdown}`)"
                except (TypeError, ValueError, OverflowError):
                    self.logger.warning(
                        "Failed to format next funding time for symbol=%s",
                        symbol,
                        exc_info=True,
                    )

            if stats:
                total = stats.get("total_hit_rate", 0)
                avg_run = stats.get("average_run_length", 0)

                value_text += f"\n期間内発生率: `{total:.1%}`"
                value_text += f"\n平均で続く回数: `{avg_run:.1f}`"

            embed["fields"].append(
                {"name": f"{icon} {symbol}", "value": value_text, "inline": True}
            )

        payload = {"embeds": [embed]}

        try:
            response = requests.post(self._discord_post_url(), json=payload, timeout=30)
            response.raise_for_status()
            self.logger.info(
                f"Sent Discord notification for {len(notifications)} symbols."
            )
            try:
                message_id = response.json().get("id")
                if message_id:
                    self._record_discord_message(message_id)
            except ValueError:
                self.logger.warning("Discord response did not contain a message id")

            # DBに記録
            for n in notifications:
                self._record_notification(
                    n["hash"],
                    n["item"]["symbol"],
                    n["item"]["funding"]["rate"],
                    n["direction"],
                )
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to send Discord notification: {e}")
            return False

    def monitor(self):
        """監視実行"""
        self.logger.info("Starting funding rate check...")
        self._cleanup_old_discord_messages()

        # 1. 異常値をフェッチ
        candidates = self.fetch_abnormal_funding_rates()

        # 2. 通知 (内部で継続性チェック・重複フィルタリングを行う)
        if candidates:
            self.send_discord_notification(candidates)
        else:
            self.logger.info("No abnormal funding rates found.")


def main():
    try:
        # 開始ログ
        print(f"CMMA Funding Rate Monitor - Started at {datetime.now().isoformat()}")

        config_path = sys.argv[1] if len(sys.argv) > 1 else None
        monitor = CmmaFundingRateMonitor(config_path)

        while True:
            monitor.monitor()

            interval = monitor.check_interval_seconds
            print(f"Waiting for {interval} seconds...")
            time.sleep(interval)

    except KeyboardInterrupt:
        print("Process interrupted.")
        sys.exit(0)
    except Exception as e:
        print(f"Critical Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
