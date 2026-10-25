"""
NEAR Wallet Tracker — Telegram bot (real-time, free).

Add a NEAR wallet, give it a name, get notified about every activity:
what it buys / sells (DEX trades), token transfers, NEAR transfers.

Data: Intear API  wss://api.intea.rs/ws  (free, no API key)
      trades.subscribe   {trader: wallet}
      transfers.subscribe {account: wallet}
Token metadata: NEAR RPC ft_metadata (free).
"""
import asyncio
import base64
import json
import logging
import os
import re
import sqlite3
import time
from decimal import Decimal, getcontext
from html import escape

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (BotCommand, CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, Message)
from dotenv import load_dotenv

getcontext().prec = 60
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WS_URL = os.getenv("INTEAR_WS", "wss://api.intea.rs/ws")
RPC_URL = os.getenv("NEAR_RPC", "https://free.rpc.fastnear.com")
MAX_WALLETS_PER_CHAT = int(os.getenv("MAX_WALLETS_PER_CHAT", "20"))
MIN_NEAR = Decimal(os.getenv("MIN_NEAR", "0.02"))   # ignore tiny NEAR-only moves (storage deposits)
FLUSH_DELAY = float(os.getenv("FLUSH_DELAY", "4"))   # seconds to collect all parts of one tx
DB_PATH = os.getenv("DB_PATH", "tracker.db")
ACCOUNTS_PER_CONN = 45                               # 2 subs per wallet, server limit 100 per connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tracker")

ACCOUNT_RE = re.compile(r"^(([a-z\d]+[-_])*[a-z\d]+\.)*([a-z\d]+[-_])*[a-z\d]+$")
TRADE_FIELDS = ["id", "transaction_id", "receipt_id", "pool_id", "token_in", "amount_in",
                "token_out", "amount_out", "usd_value", "referrer", "timestamp_millis"]
TRANSFER_FIELDS = ["id", "kind", "transaction_id", "receipt_id", "token", "sender",
                   "receiver", "amount", "timestamp_millis"]

# ---------------------------------------------------------------- texts
T = {
    "en": {
        "start": ("👋 <b>NEAR Wallet Tracker</b>\n\n"
                  "Add a wallet and I'll send you all its activity in real time: what it buys, "
                  "what it sells, token and NEAR transfers.\n\n"
                  "/add — add a wallet\n/list — my wallets\n/remove name — remove"),
        "ask_wallet": "Send a NEAR wallet address (e.g. <code>alice.near</code>):",
        "bad_wallet": "❌ That doesn't look like a NEAR address. Try again:",
        "ask_name": "What should I call this wallet? (e.g. Whale #1)",
        "added": "✅ Tracking <b>{name}</b> — <code>{acc}</code>\nI'll send every new action.",
        "exists": "This wallet is already tracked as <b>{name}</b>.",
        "limit": "Limit is {n} wallets. Remove one via /list.",
        "empty": "No wallets yet. Add one: /add",
        "list": "📋 <b>Your wallets:</b>",
        "removed": "🗑 Removed: <b>{name}</b>",
        "not_found": "No wallet with that name.",
        "cancel": "Cancelled.",
        "swap": "🔄 Swap", "buy": "🟢 Bought", "sell": "🔴 Sold",
        "recv": "📥 Received", "sent": "📤 Sent",
        "from": "from", "to": "to", "via": "via",
    },
}


def lang_of(user) -> str:
    return "en"  # bot is English-only


# ---------------------------------------------------------------- database
db = sqlite3.connect(DB_PATH)
db.execute("""CREATE TABLE IF NOT EXISTS wallets(
    chat_id INTEGER, account TEXT, name TEXT, lang TEXT,
    PRIMARY KEY(chat_id, account))""")
db.execute("""CREATE TABLE IF NOT EXISTS cursors(
    account TEXT, kind TEXT, after_id INTEGER, PRIMARY KEY(account, kind))""")
db.execute("""CREATE TABLE IF NOT EXISTS tokens(
    contract TEXT PRIMARY KEY, symbol TEXT, decimals INTEGER)""")
db.commit()


def get_cursor(acc, kind):
    row = db.execute("SELECT after_id FROM cursors WHERE account=? AND kind=?", (acc, kind)).fetchone()
    return row[0] if row else None


def set_cursor(acc, kind, after_id):
    if after_id is None:
        return
    cur = get_cursor(acc, kind)
    if cur is None or int(after_id) > cur:
        db.execute("INSERT OR REPLACE INTO cursors VALUES(?,?,?)", (acc, kind, int(after_id)))
        db.commit()


# ---------------------------------------------------------------- token metadata
session: aiohttp.ClientSession | None = None
_meta_cache: dict[str, tuple[str, int | None]] = {}


async def token_meta(contract: str) -> tuple[str, int | None]:
    if contract in ("near", "wrap.near"):
        return "NEAR", 24
    if contract in _meta_cache:
        return _meta_cache[contract]
    row = db.execute("SELECT symbol, decimals FROM tokens WHERE contract=?", (contract,)).fetchone()
    if row:
        _meta_cache[contract] = (row[0], row[1])
        return _meta_cache[contract]
    body = {"jsonrpc": "2.0", "id": "1", "method": "query",
            "params": {"request_type": "call_function", "finality": "final",
                       "account_id": contract, "method_name": "ft_metadata",
                       "args_base64": base64.b64encode(b"{}").decode()}}
    try:
        async with session.post(RPC_URL, json=body, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
        meta = json.loads(bytes(data["result"]["result"]).decode())
        res = (str(meta.get("symbol") or contract)[:20], int(meta.get("decimals") or 0))
        db.execute("INSERT OR REPLACE INTO tokens VALUES(?,?,?)", (contract, res[0], res[1]))
        db.commit()
    except Exception as e:
        log.warning("ft_metadata %s failed: %s", contract, e)
        res = (contract, None)   # not cached -> retried next time
        return res
    _meta_cache[contract] = res
    return res


def fmt(x: Decimal) -> str:
    x = abs(x)
    if x == 0:
        return "0"
    if x >= 1000:
        s = f"{x:,.0f}"
    elif x >= 1:
        s = f"{x:,.4f}".rstrip("0").rstrip(".")
    else:
        s = f"{x:.8f}".rstrip("0").rstrip(".")
    return s.replace(",", " ")


def norm_token(t):
    return "near" if t in ("near", "wrap.near") else t


# ---------------------------------------------------------------- aggregation per transaction
class Aggregator:
    """Collects trade + transfer rows of the same tx for FLUSH_DELAY seconds, then sends one message."""

    def __init__(self):
        self.buf: dict[tuple[str, str], dict] = {}
        self.bot: Bot | None = None

    def add(self, acc, kind, row):
        tx = row.get("transaction_id") or row.get("receipt_id") or f"row{row.get('id')}"
        key = (acc, tx)
        if key not in self.buf:
            self.buf[key] = {"trades": [], "transfers": [], "ts": row.get("timestamp_millis")}
            asyncio.get_running_loop().call_later(
                FLUSH_DELAY, lambda: asyncio.create_task(self.flush(key)))
        self.buf[key][kind].append(row)

    async def flush(self, key):
        ev = self.buf.pop(key, None)
        if not ev:
            return
        acc, tx = key
        try:
            data = await build_event(acc, ev)
        except Exception as e:
            log.exception("build_event %s %s: %s", acc, tx, e)
            return
        if not data:
            return
        subs = db.execute("SELECT chat_id, name, lang FROM wallets WHERE account=?", (acc,)).fetchall()
        for chat_id, name, lang in subs:
            text = render(acc, name, lang, tx, data)
            try:
                await self.bot.send_message(chat_id, text, disable_web_page_preview=True)
            except Exception as e:
                log.warning("send to %s failed: %s", chat_id, e)
            await asyncio.sleep(0.05)


agg = Aggregator()


async def build_event(acc, ev):
    deltas: dict[str, int] = {}
    counterparty: list[str] = []
    usd = Decimal(0)
    via = []

    def add(tok, amount):
        tok = norm_token(tok)
        deltas[tok] = deltas.get(tok, 0) + int(amount)

    if ev["trades"]:
        seen = set()
        for t in ev["trades"]:
            if not t.get("token_in") or not t.get("token_out"):
                continue
            k = (t.get("receipt_id"), t.get("pool_id"), t.get("token_in"), t.get("amount_in"))
            if k in seen:   # same trade seen from both tokens
                continue
            seen.add(k)
            add(t["token_in"], -int(t.get("amount_in") or 0))
            add(t["token_out"], int(t.get("amount_out") or 0))
            try:
                usd += Decimal(str(t.get("usd_value") or 0))
            except Exception:
                pass
            for v in (t.get("referrer"), t.get("pool_id")):
                if v and v not in via:
                    via.append(v)
        kind = "trade"
    else:
        for t in ev["transfers"]:
            if t.get("kind") != "transfer" or not t.get("token"):
                continue
            s, r, amt = t.get("sender"), t.get("receiver"), int(t.get("amount") or 0)
            if s == acc and r != acc:
                add(t["token"], -amt)
                if r and r not in counterparty:
                    counterparty.append(r)
            elif r == acc and s != acc:
                add(t["token"], amt)
                if s and s not in counterparty:
                    counterparty.append(s)
        kind = "transfer"

    items = []
    for tok, raw in deltas.items():
        if raw == 0:
            continue
        symbol, dec = await token_meta(tok)
        amount = Decimal(raw) / (Decimal(10) ** dec) if dec is not None else Decimal(raw)
        items.append({"token": tok, "symbol": symbol, "amount": amount, "raw": dec is None})

    # drop dust NEAR (storage deposits, gas-like moves)
    items = [i for i in items if not (i["token"] == "near" and abs(i["amount"]) < MIN_NEAR and len(items) > 1)]
    if not items:
        return None
    if len(items) == 1 and items[0]["token"] == "near" and abs(items[0]["amount"]) < MIN_NEAR:
        return None
    # counterparties that are just the token contracts themselves are not interesting
    counterparty = [c for c in counterparty if c not in deltas and c != "wrap.near"]
    return {"kind": kind, "items": items, "usd": usd, "via": via, "counterparty": counterparty}


def render(acc, name, lang, tx, d):
    tr = T["en"]
    plus = [i for i in d["items"] if i["amount"] > 0]
    minus = [i for i in d["items"] if i["amount"] < 0]

    def line(i, sign):
        sym = escape(i["symbol"])
        if i["token"] != "near":
            sym = f'<a href="https://nearblocks.io/token/{escape(i["token"])}">{sym}</a>'
        suffix = " (raw)" if i["raw"] else ""
        return f"   {sign} {fmt(i['amount'])} <b>{sym}</b>{suffix}"

    lines = [f"🔔 <b>{escape(name)}</b> · <code>{escape(acc)}</code>", ""]
    if plus and minus:
        if any(i["token"] == "near" for i in minus) and all(i["token"] != "near" for i in plus):
            head = tr["buy"]
        elif any(i["token"] == "near" for i in plus) and all(i["token"] != "near" for i in minus):
            head = tr["sell"]
        else:
            head = tr["swap"]
        lines.append(f"<b>{head}</b>")
        lines += [line(i, "−") for i in minus] + [line(i, "+") for i in plus]
        if d["usd"] > 0:
            lines.append(f"≈ ${d['usd']:,.2f}".replace(",", " "))
        where = d["via"] or d["counterparty"]
        if where:
            lines.append(f"{tr['via']} " + ", ".join(f"<code>{escape(w)}</code>" for w in where[:2]))
    elif plus:
        lines.append(f"<b>{tr['recv']}</b>")
        lines += [line(i, "+") for i in plus]
        if d["counterparty"]:
            lines.append(f"{tr['from']} " + ", ".join(f"<code>{escape(c)}</code>" for c in d["counterparty"][:2]))
    else:
        lines.append(f"<b>{tr['sent']}</b>")
        lines += [line(i, "−") for i in minus]
        if d["counterparty"]:
            lines.append(f"{tr['to']} " + ", ".join(f"<code>{escape(c)}</code>" for c in d["counterparty"][:2]))

    lines.append("")
    links = []
    if not tx.startswith("row"):
        links.append(f'<a href="https://nearblocks.io/txns/{escape(tx)}">Tx</a>')
    links.append(f'<a href="https://nearblocks.io/address/{escape(acc)}">Wallet</a>')
    lines.append(" · ".join(links))
    return "\n".join(lines)


# ---------------------------------------------------------------- Intear websocket
class Shard:
    """One websocket connection serving up to ACCOUNTS_PER_CONN wallets."""

    def __init__(self, n):
        self.n = n
        self.accounts: set[str] = set()
        self.ws = None
        self.ready = asyncio.Event()
        self.next_id = 1
        self.pending: dict[int, asyncio.Future] = {}
        self.subs: dict[int, dict] = {}          # sub id -> {acc, kind, columns}
        self.acc_subs: dict[str, list[int]] = {}
        self.lock = asyncio.Lock()
        asyncio.create_task(self.run())

    async def run(self):
        while True:
            try:
                async with session.ws_connect(WS_URL, autoping=True, heartbeat=30,
                                              compress=15, max_msg_size=0) as ws:
                    log.info("shard %s connected", self.n)
                    self.ws = ws
                    self.subs.clear()
                    self.acc_subs.clear()
                    reader = asyncio.create_task(self.reader(ws))
                    await asyncio.sleep(1)  # let the server send `hello`
                    for acc in list(self.accounts):
                        await self.subscribe(acc)
                    self.ready.set()
                    await reader
            except Exception as e:
                log.warning("shard %s ws error: %s", self.n, e)
            self.ready.clear()
            self.ws = None
            for f in self.pending.values():
                if not f.done():
                    f.set_exception(ConnectionError("ws closed"))
            self.pending.clear()
            await asyncio.sleep(5)

    async def reader(self, ws):
        while True:
            msg = await ws.receive(timeout=90)   # server sends `idle` every 15s
            if msg.type != aiohttp.WSMsgType.TEXT:
                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING,
                                aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                    return
                continue
            try:
                data = json.loads(msg.data)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            if "sub" in data:
                self.on_push(data)
            elif "id" in data and data["id"] in self.pending:
                rid = data["id"]
                if rid in self.subs and "result" in data:
                    self.on_rows(rid, data["result"])       # columns + catch-up rows
                f = self.pending.pop(rid)
                if not f.done():
                    f.set_result(data)

    async def request(self, method, params, rid=None):
        rid = rid or self.next_id
        self.next_id = max(self.next_id, rid) + 1
        fut = asyncio.get_running_loop().create_future()
        self.pending[rid] = fut
        await self.ws.send_json({"id": rid, "method": method, "params": params})
        data = await asyncio.wait_for(fut, 30)
        if "error" in data:
            raise RuntimeError(f"{method}: {data['error']}")
        return data.get("result")

    async def subscribe(self, acc):
        async with self.lock:
            if acc in self.acc_subs or not self.ws:
                return
            ids = []
            for kind, method, fields, filt in (
                    ("trades", "trades.subscribe", TRADE_FIELDS, {"trader": acc}),
                    ("transfers", "transfers.subscribe", TRANSFER_FIELDS, {"account": acc})):
                after = get_cursor(acc, kind)
                for attempt in (after, None) if after is not None else (None,):
                    params = {"fields": fields, **filt}
                    if attempt is not None:
                        params["after_id"] = attempt
                    rid = self.next_id
                    self.subs[rid] = {"acc": acc, "kind": kind, "columns": fields}
                    try:
                        res = await self.request(method, params, rid)
                        if res and res.get("latest_id") is not None and get_cursor(acc, kind) is None:
                            set_cursor(acc, kind, res["latest_id"])
                        ids.append(rid)
                        break
                    except Exception as e:
                        self.subs.pop(rid, None)
                        log.warning("subscribe %s %s (after=%s) failed: %s", acc, kind, attempt, e)
            self.acc_subs[acc] = ids
            log.info("shard %s: subscribed %s (%d subs)", self.n, acc, len(ids))

    async def unsubscribe(self, acc):
        async with self.lock:
            for sid in self.acc_subs.pop(acc, []):
                self.subs.pop(sid, None)
                try:
                    await self.request("unsubscribe", {"sub": sid})
                except Exception:
                    pass

    def on_rows(self, sid, payload):
        sub = self.subs.get(sid)
        if not sub or not isinstance(payload, dict):
            return
        if payload.get("columns"):
            sub["columns"] = payload["columns"]
        for row in payload.get("rows") or []:
            r = dict(zip(sub["columns"], row)) if isinstance(row, list) else row
            set_cursor(sub["acc"], sub["kind"], r.get("id"))
            agg.add(sub["acc"], sub["kind"], r)

    def on_push(self, data):
        sid = data["sub"]
        if "end" in data:   # e.g. too_far_behind -> resubscribe
            sub = self.subs.pop(sid, None)
            if sub:
                acc = sub["acc"]
                log.warning("sub %s ended: %s — resubscribing %s", sid, data["end"], acc)
                self.acc_subs.pop(acc, None)
                asyncio.create_task(self.subscribe(acc))
            return
        self.on_rows(sid, data)


class Manager:
    def __init__(self):
        self.shards: list[Shard] = []
        self.where: dict[str, Shard] = {}

    async def add(self, acc):
        if acc in self.where:
            return
        shard = next((s for s in self.shards if len(s.accounts) < ACCOUNTS_PER_CONN), None)
        if not shard:
            shard = Shard(len(self.shards) + 1)
            self.shards.append(shard)
        shard.accounts.add(acc)
        self.where[acc] = shard
        if shard.ready.is_set():
            await shard.subscribe(acc)

    async def remove(self, acc):
        shard = self.where.pop(acc, None)
        if shard:
            shard.accounts.discard(acc)
            if shard.ready.is_set():
                await shard.unsubscribe(acc)
        db.execute("DELETE FROM cursors WHERE account=?", (acc,))
        db.commit()


manager = Manager()


async def drop_if_unused(acc):
    left = db.execute("SELECT COUNT(*) FROM wallets WHERE account=?", (acc,)).fetchone()[0]
    if not left:
        await manager.remove(acc)


# ---------------------------------------------------------------- handlers
dp = Dispatcher()


class AddWallet(StatesGroup):
    wallet = State()
    name = State()


def normalize(acc: str) -> str | None:
    acc = acc.strip().lower()
    if 2 <= len(acc) <= 64 and ACCOUNT_RE.match(acc):
        return acc
    return None


async def save_wallet(msg: Message, acc: str, name: str):
    lang = lang_of(msg.from_user)
    tr = T[lang]
    ex = db.execute("SELECT name FROM wallets WHERE chat_id=? AND account=?",
                    (msg.chat.id, acc)).fetchone()
    if ex:
        return await msg.answer(tr["exists"].format(name=escape(ex[0])))
    count = db.execute("SELECT COUNT(*) FROM wallets WHERE chat_id=?", (msg.chat.id,)).fetchone()[0]
    if count >= MAX_WALLETS_PER_CHAT:
        return await msg.answer(tr["limit"].format(n=MAX_WALLETS_PER_CHAT))
    db.execute("INSERT INTO wallets VALUES(?,?,?,?)", (msg.chat.id, acc, name[:40], lang))
    db.commit()
    await manager.add(acc)
    await msg.answer(tr["added"].format(name=escape(name[:40]), acc=escape(acc)))


@dp.message(Command("start", "help"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer(T[lang_of(msg.from_user)]["start"])


@dp.message(Command("cancel"))
async def cmd_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer(T[lang_of(msg.from_user)]["cancel"])


@dp.message(Command("add"))
async def cmd_add(msg: Message, command: CommandObject, state: FSMContext):
    tr = T[lang_of(msg.from_user)]
    if command.args:  # /add alice.near Whale
        parts = command.args.split(maxsplit=1)
        acc = normalize(parts[0])
        if not acc:
            return await msg.answer(tr["bad_wallet"])
        if len(parts) == 2:
            return await save_wallet(msg, acc, parts[1].strip())
        await state.update_data(acc=acc)
        await state.set_state(AddWallet.name)
        return await msg.answer(tr["ask_name"])
    await state.set_state(AddWallet.wallet)
    await msg.answer(tr["ask_wallet"])


@dp.message(AddWallet.wallet, F.text)
async def got_wallet(msg: Message, state: FSMContext):
    tr = T[lang_of(msg.from_user)]
    acc = normalize(msg.text)
    if not acc:
        return await msg.answer(tr["bad_wallet"])
    await state.update_data(acc=acc)
    await state.set_state(AddWallet.name)
    await msg.answer(tr["ask_name"])


@dp.message(AddWallet.name, F.text)
async def got_name(msg: Message, state: FSMContext):
    data = await state.get_data()
    await state.clear()
    await save_wallet(msg, data["acc"], msg.text.strip())


@dp.message(Command("list"))
async def cmd_list(msg: Message):
    tr = T[lang_of(msg.from_user)]
    rows = db.execute("SELECT account, name FROM wallets WHERE chat_id=? ORDER BY name",
                      (msg.chat.id,)).fetchall()
    if not rows:
        return await msg.answer(tr["empty"])
    text = tr["list"] + "\n\n" + "\n".join(
        f"• <b>{escape(n)}</b> — <code>{escape(a)}</code>" for a, n in rows)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🗑 {n}", callback_data=f"del:{a}"[:64])] for a, n in rows])
    await msg.answer(text, reply_markup=kb)


@dp.callback_query(F.data.startswith("del:"))
async def cb_delete(cb: CallbackQuery):
    tr = T[lang_of(cb.from_user)]
    prefix = cb.data[4:]
    row = db.execute("SELECT account, name FROM wallets WHERE chat_id=? AND substr(account,1,?)=?",
                     (cb.message.chat.id, len(prefix), prefix)).fetchone()
    if row:
        db.execute("DELETE FROM wallets WHERE chat_id=? AND account=?", (cb.message.chat.id, row[0]))
        db.commit()
        await drop_if_unused(row[0])
        await cb.message.answer(tr["removed"].format(name=escape(row[1])))
    await cb.answer()


@dp.message(Command("remove"))
async def cmd_remove(msg: Message, command: CommandObject):
    tr = T[lang_of(msg.from_user)]
    key = (command.args or "").strip()
    rows = db.execute("SELECT account FROM wallets WHERE chat_id=? AND (name=? OR account=?)",
                      (msg.chat.id, key, key.lower())).fetchall()
    if not rows:
        return await msg.answer(tr["not_found"])
    db.execute("DELETE FROM wallets WHERE chat_id=? AND (name=? OR account=?)",
               (msg.chat.id, key, key.lower()))
    db.commit()
    for (acc,) in rows:
        await drop_if_unused(acc)
    await msg.answer(tr["removed"].format(name=escape(key)))


# ---------------------------------------------------------------- main
async def main():
    global session
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is not set (.env)")
    session = aiohttp.ClientSession()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    agg.bot = bot
    await bot.set_my_commands([
        BotCommand(command="add", description="Add a wallet"),
        BotCommand(command="list", description="My wallets"),
        BotCommand(command="remove", description="Remove a wallet"),
    ])
    for (acc,) in db.execute("SELECT DISTINCT account FROM wallets").fetchall():
        await manager.add(acc)
    try:
        await dp.start_polling(bot)
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
