"""
النظام الكامل: اكتشاف مينت مجاني بدأ اليوم على Ethereum + Robinhood Chain،
التحقق من كل الضوابط عبر buyer.py، تنفيذ الشراء، وإرسال إشعار تيليجرام.

تم التطوير:
  - ⚡ معالجة متوازية (asyncio.Semaphore) للتعامل مع عدة مجموعات في نفس الوقت
  - ⛓️ دعم سلاسل متعددة (Ethereum + Robinhood) مع معالجة متوازية
  - 👛 دعم 5 محافظ مع معالجة متوازية (كل محافظة على كل سلسلة)
  - 🚀 استخدام aiohttp لطلبات API أسرع من requests التقليدية
  - 💰 تحديث سعر ETH كل 60 ثانية بدلاً من 300 (تحديث أسرع)
  - 🔄 نظام إعادة محاولة ذكي: يعيد المحاولة كل ساعة لمدة 8 ساعات عند الفشل
  - 📊 تقارير محسّنة: توضح نوع المينت (مدفوع/مجاني) وحالة الأهلية

هذا الملف هو المسؤول عن:
- الاتصال بـ OpenSea Stream عبر WebSocket لرصد المينتات الجديدة
- جلب تفاصيل الدروب من OpenSea API
- التحقق من أن المينت مجاني وبدأ اليوم
- استدعاء buyer.py لتنفيذ عمليات الشراء من جميع المحافظ
- إرسال إشعارات تيليجرام بنتائج العمليات
- إعادة المحاولة تلقائيًا عند الفشل (كل ساعة لمدة 8 ساعات)
"""

# استيراد المكتبات الأساسية
import asyncio          # للبرمجة غير المتزامنة (async/await)
import json             # لتحليل بيانات JSON من API و WebSocket
import logging          # لتسجيل الأحداث والأخطاء
log = logging.getLogger(__name__)
import os               # للوصول إلى متغيرات البيئة
import time             # للتعامل مع الوقت والتخزين المؤقت
from datetime import datetime, timezone, timedelta  # للتعامل مع التواريخ والمناطق الزمنية
from dataclasses import dataclass, field  # لإنشاء كلاس بسيط لتتبع المحاولات

# مكتبات خارجية للاتصالات غير المتزامنة
import aiohttp          # مكتبة HTTP غير متزامنة (أسرع من requests)
import websockets       # للاتصال بـ WebSocket الخاص بـ OpenSea
from dotenv import load_dotenv  # لتحميل متغيرات البيئة من ملف .env

# استيراد الدوال والإعدادات من buyer.py
from buyer import (
    get_web3_from_config,        # إنشاء اتصال Web3
    attempt_purchase,            # تنفيذ عملية الشراء
    CHAINS_CONFIG,               # إعدادات السلاسل المدعومة
    quick_checks,                # الفحوصات السريعة (الرصيد، الغاز، الرسوم)
    check_eligibility_reason,    # تحليل سبب الفشل وتحديد الأهلية
    is_paid_mint,                # التحقق من نوع المينت (مدفوع/مجاني)
    RETRYABLE_REASONS,           # أسباب الفشل القابلة لإعادة المحاولة
    MIN_BALANCE_RESERVE_USD,     # الحد الأدنى للرصيد - للرسائل
    MAX_GAS_FEE_USD,             # الحد الأقصى لرسوم الغاز - للرسائل
)

# تحميل متغيرات البيئة من ملف .env (إن وجد)
load_dotenv()

# ===================================================================
# المتغيرات العامة (تقرأ من متغيرات البيئة)
# ===================================================================
# كل هذه القيم يجب تعبئتها في ملف .env قبل تشغيل النظام

# مفاتيح API الأساسية
OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"].strip()           # مفتاح OpenSea API
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"].strip()     # توكن بوت تيليجرام
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"].strip()         # معرف الدردشة في تيليجرام
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").strip().lower() == "true"  # تفعيل/إيقاف البوت

# ===================================================================
# إعداد المحافظ المتعددة (حتى 5 محافظ)
# ===================================================================
# النظام يدعم 5 محافظ كحد أقصى. يتم قراءة المحافظ من متغيرات البيئة.
# المحفظة الأولى تستخدم: PRIVATE_KEY + WALLET_ADDRESS
# المحافظ الإضافية تستخدم: WALLET_2_PRIVATE_KEY + WALLET_2_ADDRESS
#                     و: WALLET_3_PRIVATE_KEY + WALLET_3_ADDRESS ... إلخ
#
# مثال لملف .env (3 محافظ):
#   PRIVATE_KEY=0xabc...           # المحفظة 1
#   WALLET_ADDRESS=0x123...
#   WALLET_2_PRIVATE_KEY=0xdef...  # المحفظة 2
#   WALLET_2_ADDRESS=0x456...
#   WALLET_3_PRIVATE_KEY=0x789...  # المحفظة 3
#   WALLET_3_ADDRESS=0xabc...

def load_wallets():
    """
    قراءة جميع المحافظ المدعومة (حتى 5) من متغيرات البيئة.
    
    المحفظة 1 إجبارية (PRIVATE_KEY + WALLET_ADDRESS)
    المحفظة 2-5 اختيارية (WALLET_2_PRIVATE_KEY + WALLET_2_ADDRESS ...)
    
    لكل محفظة نحتاج:
    - name: اسم المحفظة (للعرض في الإشعارات)
    - private_key: المفتاح الخاص (يبدأ بـ 0x)
    - address: العنوان العام (يبدأ بـ 0x)
    
    المخرجات:
        list[dict] - قائمة المحافظ النشطة
    """
    wallets = []
    
    # قائمة بأزواج متغيرات البيئة لكل محفظة
    wallet_configs = [
        ("السمبتيك 1", "PRIVATE_KEY", "WALLET_ADDRESS"),
        ("علاء", "WALLET_2_PRIVATE_KEY", "WALLET_2_ADDRESS"),
        ("مبروك 3", "WALLET_3_PRIVATE_KEY", "WALLET_3_ADDRESS"),
        ("جواد 4", "WALLET_4_PRIVATE_KEY", "WALLET_4_ADDRESS"),
        ("ايهاب 5", "WALLET_5_PRIVATE_KEY", "WALLET_5_ADDRESS"),
        
    ]
    
    for name, pk_var, addr_var in wallet_configs:
        private_key = os.environ.get(pk_var)
        address = os.environ.get(addr_var)
        if private_key and address:
            wallets.append({
                "name": name,
                "private_key": private_key,
                "address": address,
            })
            log.info(f"✅ {name}: {address[:10]}... تم التحميل")
    
    return wallets

# تحميل جميع المحافظ
WALLETS = load_wallets()
if not WALLETS:
    logging.warning("⚠️ لم يتم العثور على أي محفظة! تأكد من تعبئة PRIVATE_KEY و WALLET_ADDRESS في ملف .env")
else:
    logging.info(f"👛 تم تحميل {len(WALLETS)} محفظة")

# روابط RPC للسلاسل المختلفة (اختياري - يمكن ترك أحدها فارغًا)
# .strip() مهمة لإزالة أي \n أو مسافات في نهاية القيمة (تسبب %0A وخطأ 401 مع Alchemy)
ROBINHOOD_RPC_URL = os.environ.get("ROBINHOOD_RPC_URL", "").strip()   # RPC لـ Robinhood Chain
ETHEREUM_RPC_URL = os.environ.get("ETHEREUM_RPC_URL", "").strip()     # RPC لـ Ethereum

# تفعيل السلاسل المتاحة
# نتحقق من وجود RPC URL لكل سلسلة ونضيفها إلى القائمة النشطة
ENABLED_CHAINS = []
if ROBINHOOD_RPC_URL:
    CHAINS_CONFIG["robinhood"]["rpc_url"] = ROBINHOOD_RPC_URL  # نضبط الـ RPC
    ENABLED_CHAINS.append("robinhood")                          # نضيف السلسلة
if ETHEREUM_RPC_URL:
    CHAINS_CONFIG["ethereum"]["rpc_url"] = ETHEREUM_RPC_URL    # نضبط الـ RPC
    ENABLED_CHAINS.append("ethereum")                           # نضيف السلسلة

# ===================================================================
# ثوابت الاتصال بالخدمات الخارجية
# ===================================================================
# هذه العناوين ثابتة ولا تتغير

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"  # رابط WebSocket
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"  # رابط API تيليجرام
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"              # رابط API الدروب

# عنوان الصفر (0x0...) - نستخدمه للتحقق من أن الناقل هو العقد (مينت جديد)
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# ===================================================================
# روابط مستكشفات البلوكشين لكل سلسلة
# ===================================================================
# تستخدم لإنشاء رابط المعاملة (tx) على المستكشف
# حتى يتمكن المستخدم من فتح الرابط ومعرفة سبب الفشل بنفسه

EXPLORER_URLS = {
    "ethereum": "https://etherscan.io/tx/",
    "robinhood": "https://explorer.robinhood.org/tx/",
}

# تعريف المنطقة الزمنية المحلية (UTC+3)
LOCAL_TZ = timezone(timedelta(hours=3))

# ===================================================================
# إعدادات الأداء والأمان
# ===================================================================
# هذه القيم تتحكم بسرعة وأداء النظام

MAX_CONCURRENT_MINTS = 3   # حد أقصى 3 مجموعات تتم معالجتها في نفس الوقت
HEARTBEAT_INTERVAL = 20       # إرسال نبضات حياة كل 20 ثانية للحفاظ على اتصال WebSocket
RECV_TIMEOUT = 5              # timeout لاستقبال الرسائل من WebSocket (ثوانٍ)
FREE_PRICE_THRESHOLD_USD = 0.000000001 # أقل من هذا السعر بالدولار = "مجاني عمليًا"
ETH_PRICE_CACHE_TTL = 3       # تحديث سعر ETH كل 60 ثانية (بدلاً من 300 في الإصدار القديم)
SCAN_INTERVAL = 50          # الماسح الدوري: فحص المينتات النشطة كل 15 ثانية

# ===================================================================
# إعدادات نظام إعادة المحاولة (Retry System)
# ===================================================================

# --- إعدادات عامة ---
MAX_RETRY_HOURS = 8               # المدة القصوى لإعادة المحاولة (8 ساعات)
RETRY_INTERVAL = 1800            # الفاصل الزمني الافتراضي (3600 ثانية = ساعة واحدة)
MAX_RETRIES_PER_TASK = MAX_RETRY_HOURS  # أقصى عدد محاولات افتراضي = 8

# --- إعدادات إعادة المحاولة حسب نوع الفشل ---
# 1. رسوم الغاز مرتفعة → كل دقيقة لمدة 7 محاولات كحد أقصى
GAS_RETRY_INTERVAL = 10             # كل 5 ثانية = دقيقة واحدة
GAS_RETRY_MAX_ATTEMPTS = 10          # أقصى 7 محاولات

# 2. المينت مدفوع → كل 5 دقائق لحين يصبح مجاني (لمدة 8 ساعات)
PAID_MINT_RETRY_INTERVAL = 120        # كل 300 ثانية = 5 دقائق

# 3. غير مؤهل (الرصيد منخفض) → كل 30 دقيقة لمدة 8 ساعات
ELIGIBILITY_RETRY_INTERVAL = 10800     # كل 1800 ثانية = 30 دقيقة


# ===================================================================
# إعدادات فحص الرصيد
# ===================================================================
BALANCE_CHECK_INTERVAL = 3600  # فحص الرصيد كل ساعة (3600 ثانية)
# رسائل إعادة المحاولة
RETRY_MESSAGES = {
    "retry_starting": "🔄 <b>جاري إعادة المحاولة</b>\n\nالمجموعة: <b>{name}</b>\nالسلسلة: {chain}\nالمحفظة: {wallet}\nالمحاولة: {attempt}/{max_retries}\nالسبب السابق: {reason}\nالوقت المتبقي: {hours_remaining} ساعة",
    "retry_expired": "⏰ <b>انتهت محاولات إعادة الشراء</b>\n\nالمجموعة: <b>{name}</b>\nالسلسلة: {chain}\nالمحفظة: {wallet}\nعدد المحاولات: {attempts}\nالمدة: {hours} ساعات\nتوقف النظام عن المحاولة لهذه العملية.",
    "retry_success": "✅ <b>تم الشراء بنجاح بعد إعادة المحاولة!</b>\n\nالمجموعة: <b>{name}</b>\nالسلسلة: {chain}\nالمحفظة: {wallet}\nالمحاولة: {attempt}/{max_retries}\nالكمية: {quantity}\nرسوم الغاز: ${gas_fee:.4f}\nمعاملة: {tx_hash}",
}


# ===================================================================
# ===================================================================
# كلاس تتبع المحاولات لكل (slug, wallet_address, chain_name)
# ===================================================================
# هذا الكلاس يتتبع لكل مهمة فاشلة:
# - عدد المحاولات
# - وقت بدء المحاولات
# - بيانات الدروب (للرجوع إليها عند إعادة المحاولة)

@dataclass
class RetryTracker:
    """تتبع محاولات إعادة الشراء لمهمة واحدة."""
    slug: str
    wallet_address: str
    chain_name: str
    detail: dict
    wallet_name: str
    wallet_private_key: str
    price_wei: int
    max_per_wallet: int | None
    remaining_supply: int
    eth_price_usd: float
    start_time: float = field(default_factory=time.time)
    attempt_count: int = 1
    original_reason: str = ""                  # سبب الفشل الأصلي
    
    @property
    def hours_passed(self) -> float:
        """حساب عدد الساعات التي مضت منذ بدء المحاولات."""
        return (time.time() - self.start_time) / 3600
    
    @property
    def is_expired(self) -> bool:
        """التحقق مما إذا كانت المدة قد انتهت (8 ساعات)."""
        return self.hours_passed >= MAX_RETRY_HOURS
    
    @property
    def hours_remaining(self) -> float:
        """حساب الساعات المتبقية."""
        remaining = MAX_RETRY_HOURS - self.hours_passed
        return max(0, remaining)
    
    @property
    def retry_key(self) -> str:
        """مفتاح فريد للمهمة (slug + wallet + chain)."""
        return f"{self.slug}:{self.wallet_address}:{self.chain_name}"


# قاموس لتتبع جميع مهام إعادة المحاولة
retry_tasks: dict[str, RetryTracker] = {}
# قفل لحماية قاموس إعادة المحاولة من الوصول المتزامن
retry_lock = asyncio.Lock()

# ===================================================================
# إعدادات تسجيل الأحداث (Logging)
# ===================================================================
# نضبط تنسيق الرسائل ليكون مقروءًا وسهل التتبع

logging.basicConfig(
    level=logging.INFO,                                 # مستوى التسجيل: INFO فما فوق
    format="%(asctime)s | %(levelname)s | %(message)s", # تنسيق: وقت | مستوى | رسالة
    datefmt="%H:%M:%S",                                 # تنسيق الوقت: ساعة:دقيقة:ثانية
)
logger = logging.getLogger("auto-buyer-v2")  # اسم المسجل

# ===================================================================
# إنشاء اتصالات Web3 لكل سلسلة نشطة
# ===================================================================
# نقوم بإنشاء كائن Web3 لكل سلسلة (Ethereum و/أو Robinhood)
# حتى نتمكن من إرسال المعاملات عليها

w3_instances = {}
for chain_name in ENABLED_CHAINS:
    try:
        # إنشاء اتصال Web3 من إعدادات السلسلة
        w3_instances[chain_name] = get_web3_from_config(CHAINS_CONFIG[chain_name])
        logging.info(f"✅ {CHAINS_CONFIG[chain_name]['chain_name_display']} - Web3 متصل")
    except Exception as e:
        logging.error(f"❌ {CHAINS_CONFIG[chain_name]['chain_name_display']} - فشل الاتصال: {e}")

# كائن Semaphore للتحكم في التزامن (يمنع تنفيذ أكثر من MAX_CONCURRENT_MINTS عملية في وقت واحد)
semaphore = asyncio.Semaphore(MAX_CONCURRENT_MINTS)

# ===================================================================
# ذاكرة تخزين مؤقت لسعر ETH
# ===================================================================
# نخزن السعر ووقت التخزين لتجنب استدعاء API بشكل متكرر

_eth_price_cache = {"value": None, "ts": 0}  # القيمة ووقت التخزين (timestamp)


async def get_eth_price_usd(session: aiohttp.ClientSession) -> float:
    """
    جلب سعر ETH من CoinGecko مع تخزين مؤقت.
    
    هذه الدالة:
    1. تتحقق أولاً من وجود قيمة مخزنة حديثة (أقل من ETH_PRICE_CACHE_TTL ثانية)
    2. إذا كانت موجودة، تعيدها فورًا (بدون طلب API)
    3. إذا لم تكن موجودة أو منتهية الصلاحية، تجلب السعر من CoinGecko
    
    المعاملات:
        session: aiohttp.ClientSession - جلسة HTTP نشطة
    
    المخرجات:
        float - سعر ETH الحالي بالدولار
    """
    now = time.time()
    
    # التحقق من وجود قيمة مخزنة حديثة
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < ETH_PRICE_CACHE_TTL):
        return _eth_price_cache["value"]  # نعيد القيمة المخزنة
    
    try:
        # طلب سعر ETH من CoinGecko API
        async with session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=aiohttp.ClientTimeout(total=8),  # timeout 8 ثوانٍ كحد أقصى
        ) as resp:
            if resp.status == 200:
                data = await resp.json()           # تحليل الاستجابة
                # التحقق من وجود المفتاح ethereum في الاستجابة
                if "ethereum" in data and "usd" in data["ethereum"]:
                    price = data["ethereum"]["usd"]     # استخراج السعر
                    _eth_price_cache["value"] = price   # تخزين القيمة
                    _eth_price_cache["ts"] = now        # تخزين وقت التحديث
                    logging.info(f"[السعر] تم تحديث سعر ETH: ${price}")
                    return price
                else:
                    logging.warning(f"[السعر] استجابة CoinGecko غير متوقعة: {data}")
            else:
                logging.warning(f"[السعر] CoinGecko رد بـ HTTP {resp.status}")
    except Exception as e:
        # في حالة الخطأ، نستخدم آخر سعر معروف أو 3000 كقيمة افتراضية
        logging.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
    
    # نستخدم آخر سعر معروف أو 3000 كقيمة افتراضية
    return _eth_price_cache["value"] or 3000.0


# ===================================================================
# دوال التعامل مع OpenSea API
# ===================================================================
# هذه الدوال تستخدم aiohttp للتواصل مع OpenSea API بشكل غير متزامن

async def fetch_drop_detail_async(session: aiohttp.ClientSession, slug: str):
    """
    جلب تفاصيل الدروب (المينت) من OpenSea API باستخدام aiohttp.
    
    هذه الدالة غير متزامنة => أسرع من استخدام requests التقليدية.
    
    المعاملات:
        session: aiohttp.ClientSession - جلسة HTTP نشطة
        slug: str - المعرف الفريد للمجموعة (collection slug)
    
    المخرجات:
        tuple: (found: bool | None, detail: dict | None)
        - found: True إذا وجد، False إذا 404، None إذا خطأ
        - detail: بيانات الدروب إذا وجد، None إذا لم يوجد
    """
    # بناء رابط API مع الـ slug
    url = f"{DROPS_API_BASE}/{slug}"
    headers = {"x-api-key": OPENSEA_API_KEY}  # المصادقة بمفتاح API
    
    try:
        # إرسال طلب GET غير متزامن
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                return True, await resp.json()   # نجاح: نعيد البيانات
            if resp.status == 404:
                return False, None               # غير موجود
            return None, None                     # حالة أخرى غير متوقعة
    except Exception as e:
        logging.warning(f"[Drops API] خطأ: {e}")
        return None, None


def parse_iso(ts: str):
    """
    تحليل نص تاريخ بتنسيق ISO 8601 إلى كائن datetime.
    
    المعاملات:
        ts: str - النص التاريخي (مثل "2024-01-01T12:00:00Z")
    
    المخرجات:
        datetime | None - كائن datetime أو None إذا فشل التحليل
    """
    try:
        # استبدال Z بـ +00:00 للتوافق مع fromisoformat
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None  # فشل التحليل


def started_today_local(stage: dict) -> bool:
    """
    التحقق مما إذا كانت مرحلة المينت قد بدأت اليوم بالتوقيت المحلي.
    
    المعاملات:
        stage: dict - بيانات المرحلة (تحتوي على start_time)
    
    المخرجات:
        bool - True إذا بدأت اليوم، False إذا لم تبدأ أو فشل التحليل
    """
    # استخراج وقت البدء من بيانات المرحلة
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False  # لا يوجد وقت بدء صالح
    
    # مقارنة تاريخ البدء (بالتوقيت المحلي) مع تاريخ اليوم
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()


def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    """
    التحقق مما إذا كان السعر مجانيًا أو مهملاً (أقل من العتبة).
    
    المعاملات:
        price_wei: int - السعر بـ Wei (أصغر وحدة في ETH)
        eth_price_usd: float - سعر ETH الحالي
    
    المخرجات:
        bool - True إذا كان مجانيًا أو أقل من العتبة
    """
 
   # إذا كان السعر 0 أو أقل، فهو مجاني
    if price_wei == 0:
        return True
    
    # تحويل السعر إلى دولار ومقارنته بالعتبة
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD
# ===================================================================
# نظام إشعارات تيليجرام (غير متزامن)
# ===================================================================
# نستخدم طابور (Queue) غير متزامن لإرسال الرسائل بشكل منظم
# هذا يمنع إغراق API تيليجرام بالطلبات المتزامنة

# طابور الرسائل: نضيف رسائل إليه، وترسل الدالة telegram_sender الرسائل بالتدريج
send_queue: asyncio.Queue[str] = asyncio.Queue()


def enqueue_message(text: str):
    """
    إضافة رسالة إلى طابور الإرسال.
    
    هذه الدالة آمنة للاستخدام من أي مكان في الكود.
    الرسالة سترسل تلقائيًا عبر telegram_sender.
    
    المعاملات:
        text: str - نص الرسالة المراد إرسالها
    """
    send_queue.put_nowait(text)


async def telegram_sender():
    """
    إرسال الرسائل من الطابور إلى تيليجرام بشكل غير متزامن.
    
    هذه الدالة:
    1. تنتظر ظهور رسالة في الطابور
    2. ترسلها إلى تيليجرام
    3. تنتظر ثانية قبل إرسال الرسالة التالية (تجنب الـ rate limit)
    """
    # إنشاء جلسة HTTP مشتركة (أكثر كفاءة)
    async with aiohttp.ClientSession() as session:
        while True:
            # انتظار رسالة جديدة في الطابور
            text = await send_queue.get()
            
            try:
                # إرسال الرسالة إلى تيليجرام
                async with session.post(
                    f"{TELEGRAM_API}/sendMessage",
                    data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": text,
                        "parse_mode": "HTML",  # دعم HTML في الرسائل
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        logging.warning(f"تيليجرام: استجابة غير متوقعة {resp.status}")
            except Exception as e:
                logging.error(f"خطأ إرسال تليجرام: {e}")
            
            # إعلام الطابور بإتمام المعالجة
            send_queue.task_done()
            
            # انتظار 1.05 ثانية قبل الرسالة التالية (تجنب rate limiting)
            await asyncio.sleep(1.05)


# ===================================================================
# رسائل أسباب الفشل للعرض في تيليجرام
# ===================================================================
# هذه القاموس يترجم أسباب الفشل التقنية إلى نصوص عربية مفهومة

REASON_MESSAGES = {
    "balance_too_low": "الرصيد بالمحفظة منخفض جدًا — توقف النظام عن الشراء",
    "gas_too_high": "رسوم الغاز التقديرية تجاوزت الحد المسموح",
    "gas_too_high_precise": "رسوم الغاز الفعلية (بعد التقدير الدقيق) تجاوزت الحد",
    "no_fee_recipient": "تعذر تحديد عنوان الرسوم من العقد",
    "simulation_failed": "محاكاة المعاملة فشلت — على الأغلب المينت غير متاح فعليًا",
    "insufficient_funds_for_total_cost": "الرصيد لا يكفي سعر المينت + الغاز معًا",
    "tx_value_too_high": "قيمة المعاملة تجاوزت الحد الأقصى المسموح به",
    "tx_error": "خطأ أثناء إرسال المعاملة",
}


# دوال مساعدة لتحليل نوع المينت وأهلية المستخدم
def check_mint_type(price_wei: int, eth_price_usd: float) -> dict:
    """
    تحديد نوع المينت (مدفوع/مجاني) وعرض السعر.
    
    المعاملات:
        price_wei: int - السعر بـ Wei
        eth_price_usd: float - سعر ETH الحالي
    
    المخرجات:
        dict: {mint_type: str, price_usd: float, icon: str}
    """
    paid, price, label = is_paid_mint(price_wei, eth_price_usd)
    return {
        "mint_type": label,
        "price_usd": price,
        "icon": "💰" if paid else "🎁",
    }


def check_user_eligibility(reason: str, balance_usd: float = 0, gas_fee_usd: float = 0) -> dict:
    """
    تحديد أهلية المستخدم بناءً على سبب الفشل.
    
    المعاملات:
        reason: str - سبب الفشل
        balance_usd: float - الرصيد الحالي
        gas_fee_usd: float - رسوم الغاز المقدرة
    
    المخرجات:
        dict: {
            eligible: bool,       # هل المستخدم مؤهل؟
            icon: str,            # أيقونة (مؤهل/غير مؤهل)
            label: str,           # نص وصفي
            description: str,     # وصف تفصيلي
            retryable: bool,      # هل يستحق إعادة المحاولة؟
        }
    """
    # استخدام دالة التحليل من buyer.py
    analysis = check_eligibility_reason(reason)
    
    # بناء النص الوصفي حسب نوع المشكلة
    if analysis["eligible"]:
        icon = "✅"
        label = "مؤهل ✅"
    else:
        icon = "❌"
        label = "غير مؤهل ❌"
    
    # إضافة معلومات إضافية حسب نوع المشكلة
    extra_info = ""
    if reason == "balance_too_low":
        extra_info = f"\n📊 رصيدك الحالي: ${balance_usd:.4f} — الحد الأدنى المطلوب: ${MIN_BALANCE_RESERVE_USD}"
    elif "gas_too_high" in reason:
        extra_info = f"\n⛽ رسوم الغاز المقدرة: ${gas_fee_usd:.4f} — الحد الأقصى المسموح: ${MAX_GAS_FEE_USD}"
    
    return {
        "eligible": analysis["eligible"],
        "icon": icon,
        "label": label,
        "description": analysis["description"] + extra_info,
        "retryable": analysis["retryable"],
    }


def build_result_message(detail: dict, result: dict, chain_name: str, wallet_name: str = "المحفظة") -> str:
    """
    بناء رسالة تيليجرام مناسبة بناءً على نتيجة العملية (نجاح أو فشل).
    عند الفشل، تتضمن معلومات: نوع المينت (مدفوع/مجاني) وحالة الأهلية.
    
    المعاملات:
        detail: dict - بيانات الدروب (اسم المجموعة، الرابط، إلخ)
        result: dict - نتيجة عملية الشراء من buyer.py
        chain_name: str - اسم السلسلة (robinhood أو ethereum)
        wallet_name: str - اسم المحفظة (مثل "المحفظة 1")
    
    المخرجات:
        str - نص الرسالة المنسق بتنسيق HTML
    """
    # استخراج اسم المجموعة ورابطها
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_display = CHAINS_CONFIG.get(chain_name, {}).get("chain_name_display", chain_name)
    
    # استخراج السعر (إن وجد في التفاصيل)
    stage = detail.get("active_stage") or {}
    price_wei = int(stage.get("price", "0"))
    eth_price_usd = result.get("eth_price_usd", 0)

    # إذا نجحت العملية => رسالة نجاح
    if result["success"]:
        return (
            f"✅ <b>تم الشراء بنجاح!</b>\n\n"
            f"المجموعة: <b>{name}</b>\n"
            f"السلسلة: {chain_display}\n"
            f"المحفظة: {wallet_name}\n"
            f"الكمية: {result['quantity']}\n"
            f"رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
            f"معاملة: {result['tx_hash']}\n"
            f"🔗 {url}"
        )

    # إذا فشلت => رسالة إلغاء مع ذكر السبب + معلومات الدفع والأهلية
    reason = result.get("reason", "unknown")
    reason_text = REASON_MESSAGES.get(reason, reason)
    
    # معلومات الدفع (مدفوع/مجاني)
    mint_info = check_mint_type(price_wei, eth_price_usd)
    
    # معلومات الأهلية
    eligibility = check_user_eligibility(
        reason, 
        balance_usd=result.get("balance_usd", 0),
        gas_fee_usd=result.get("gas_fee_usd", 0),
    )
    
    # إضافة تفاصيل إضافية حسب سبب الفشل
    extra = ""
    if result.get("balance_usd"):
        extra += f"\nالرصيد الحالي: ${result['balance_usd']:.4f}"
    if result.get("gas_fee_usd"):
        extra += f"\nالرسوم المقدّرة: ${result['gas_fee_usd']:.4f}"
    if result.get("tx_hash"):
        extra += f"\nهاش المعاملة: {result['tx_hash']}"

    return (
        f"⏭️ <b>تم تجاهل الشراء</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"السلسلة: {chain_display}\n"
        f"المحفظة: {wallet_name}\n"
        f"السبب: {reason_text}{extra}\n\n"
        f"─── ℹ️ معلومات التحليل ───\n"
        f"{mint_info['icon']} نوع المينت: {mint_info['mint_type']}\n"
        f"{eligibility['icon']} الأهلية: {eligibility['label']}\n"
        f"📝 {eligibility['description']}\n"
        f"🔗 {url}"
    )


def build_mint_info_message(detail: dict, eth_price_usd: float) -> str:
    """
    بناء رسالة تفصيلية بمعلومات المينت لعرضها في تيليجرام قبل أي إجراء.

    تعرض: اسم المجموعة، نوع المينت (مدفوع/مجاني)، السعر، الكمية الكلية،
    الكمية المتبقية، الحد لكل محفظة، تاريخ البدء والانتهاء، ورابط OpenSea.

    المعاملات:
        detail: dict - بيانات الدروب من OpenSea API
        eth_price_usd: float - سعر ETH الحالي بالدولار

    المخرجات:
        str - نص الرسالة المنسق بتنسيق HTML
    """
    name = detail.get("collection_name") or detail.get("collection_slug", "غير معروف")
    url = detail.get("opensea_url", "")

    stage = detail.get("active_stage") or {}
    price_wei = int(stage.get("price", "0") or "0")

    # تحديد السعر ونوع المينت
    paid, price_usd, _label = is_paid_mint(price_wei, eth_price_usd)
    price_eth = price_wei / 1e18 if price_wei > 0 else 0
    if paid:
        price_str = f"💰 مدفوع: {price_eth:.4f} ETH (≈ ${price_usd:.2f})"
        header_icon = "💰"
        mint_type = "مدفوع"
        action_note = "⏭️ مينت مدفوع — سيتم التجاهل تلقائياً"
    else:
        price_str = "🎁 مجاني"
        header_icon = "🎁"
        mint_type = "مجاني"
        action_note = "⏳ جاري الشراء التلقائي..."

    # تنسيق التواريخ بالتوقيت المحلي
    def fmt_date(ts: str) -> str:
        if not ts:
            return "غير محدد"
        dt = parse_iso(ts)
        if not dt:
            return ts[:16] if len(ts) >= 16 else ts
        return dt.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")

    start_str = fmt_date(stage.get("start_time", ""))
    end_str = fmt_date(stage.get("end_time", ""))

    # الكميات
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max(0, max_supply - total_supply)

    # الحد الأقصى لكل محفظة
    max_per_wallet_raw = stage.get("max_per_wallet")
    max_per_wallet_str = str(max_per_wallet_raw) if max_per_wallet_raw is not None else "غير محدد"

    return (
        f"{header_icon} <b>مينت {mint_type} نشط!</b>\n\n"
        f"📦 المجموعة: <b>{name}</b>\n"
        f"💲 السعر: {price_str}\n"
        f"📊 الكمية الكلية: {max_supply:,}\n"
        f"✅ المتبقي: {remaining:,} قطعة\n"
        f"👛 الحد لكل محفظة: {max_per_wallet_str}\n"
        f"🕐 بدأ: {start_str}\n"
        f"🕔 ينتهي: {end_str}\n"
        f"ℹ️ {action_note}\n"
        f"🔗 {url}"
    )


# ===================================================================
# دوال نظام إعادة المحاولة (Retry System)
# ===================================================================
# ===================================================================
# إعدادات إعادة المحاولة حسب نوع الفشل
# ===================================================================

# 1. رسوم الغاز مرتفعة → كل دقيقة لمدة 7 محاولات كحد أقصى
GAS_RETRY_INTERVAL = 5               # كل 60 ثانية = دقيقة واحدة
GAS_RETRY_MAX_ATTEMPTS = 7            # أقصى 7 محاولات

# 2. المينت مدفوع → كل 5 دقائق لحين يصبح مجاني (لمدة 8 ساعات)
PAID_MINT_RETRY_INTERVAL = 300        # كل 300 ثانية = 5 دقائق

# 3. غير مؤهل (الرصيد منخفض) → كل 30 دقيقة لمدة 8 ساعات
ELIGIBILITY_RETRY_INTERVAL = 1800     # كل 1800 ثانية = 30 دقيقة





async def retry_purchase(session: aiohttp.ClientSession, tracker: RetryTracker):
    """
    تنفيذ محاولة شراء جديدة بناءً على بيانات tracker.
    تستخدم attempt_purchase مباشرة لهذه المحفظة والسلسلة.
    """
    found, detail = await fetch_drop_detail_async(session, tracker.slug)
    if not found or not detail:
        logging.warning(f"⏭️ إعادة المحاولة: لم نجد الدروب {tracker.slug}")
        async with retry_lock:
            retry_tasks.pop(tracker.retry_key, None)
        return

    stage = detail.get("active_stage") or {}
    tracker.price_wei = int(stage.get("price", "0") or "0")
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    tracker.remaining_supply = max(0, max_supply - total_supply)
    tracker.detail = detail

    eth_price_usd = await get_eth_price_usd(session)
    tracker.eth_price_usd = eth_price_usd

    w3 = w3_instances.get(tracker.chain_name)
    if not w3:
        logging.warning(f"⚠️ لا يوجد Web3 للسلسلة {tracker.chain_name}")
        return

    chain_config = CHAINS_CONFIG[tracker.chain_name]
    seadrop_address = chain_config["seadrop_address"]
    contract_address = detail.get("contract_address")
    if not contract_address:
        logging.warning(f"⚠️ لا يوجد contract_address في الدروب {tracker.slug}")
        return

    checks = quick_checks(
        w3, tracker.wallet_address, eth_price_usd,
        contract_address, seadrop_address,
    )
    if not checks["pass"]:
        reason = checks["reason"]
        logging.info(f"⏭️ إعادة المحاولة '{tracker.slug}' - {tracker.wallet_name}: {reason}")
        if reason not in RETRYABLE_REASONS:
            async with retry_lock:
                retry_tasks.pop(tracker.retry_key, None)
        else:
            tracker.original_reason = reason
        return

    result = await asyncio.to_thread(
        attempt_purchase,
        w3,
        tracker.wallet_private_key,
        tracker.wallet_address,
        contract_address,
        seadrop_address,
        tracker.price_wei,
        tracker.max_per_wallet,
        tracker.remaining_supply,
        eth_price_usd,
    )
    result["eth_price_usd"] = eth_price_usd
    enqueue_message(build_result_message(detail, result, tracker.chain_name, tracker.wallet_name))

    if result.get("success"):
        async with retry_lock:
            retry_tasks.pop(tracker.retry_key, None)
        logging.info(f"✅ إعادة المحاولة نجحت لـ '{tracker.slug}' - {tracker.wallet_name}")
    else:
        reason = result.get("reason", "غير معروف")
        if reason not in RETRYABLE_REASONS:
            async with retry_lock:
                retry_tasks.pop(tracker.retry_key, None)
        else:
            tracker.original_reason = reason




async def schedule_retry(session: aiohttp.ClientSession, tracker: RetryTracker):
    """
    جدولة إعادة المحاولة وفقًا لسبب الفشل.
    - للغاز: كل 5 ثوانٍ (GAS_RETRY_INTERVAL) لضمان المحاولة المستمرة.
    - لعنوان الرسوم/المحاكاة: كل دقيقة لضمان السرعة.
    - للرصيد المنخفض: كل 30 دقيقة.
    تستمر المحاولات حتى انتهاء المينت أو مرور 8 ساعات.
    """
    while not tracker.is_expired:
        # تحديد الفاصل الزمني بناءً على السبب الحالي
        reason = tracker.original_reason.lower()
        if "gas" in reason:
            interval = GAS_RETRY_INTERVAL  # 5 ثوانٍ
        elif reason in ("no_fee_recipient", "simulation_failed"):
            interval = 60  # دقيقة واحدة لهذه الحالات التقنية
        elif reason == "balance_too_low":
            interval = ELIGIBILITY_RETRY_INTERVAL # 30 دقيقة
        else:
            interval = RETRY_INTERVAL # ساعة

        await asyncio.sleep(interval)
        
        # قبل المحاولة، نتحقق هل المينت ما زال نشطاً؟
        found, detail = await fetch_drop_detail_async(session, tracker.slug)
        if not found or not detail or not detail.get("is_minting"):
            logging.info(f"⏹️ توقف إعادة المحاولة لـ '{tracker.slug}': المينت انتهى أو غير نشط.")
            async with retry_lock:
                retry_tasks.pop(tracker.retry_key, None)
            break

        tracker.attempt_count += 1
        # إرسال إشعار دوري فقط كل 10 محاولات للغاز، أو كل مرة للأسباب الأخرى لتقليل الإزعاج
        if "gas" not in reason or tracker.attempt_count % 10 == 0:
            enqueue_message(
                f"🔄 <b>إعادة محاولة مستمرة</b>\n\n"
                f"المجموعة: <b>{tracker.slug}</b>\n"
                f"السبب: {tracker.original_reason}\n"
                f"المحاولة: {tracker.attempt_count}\n"
                f"الوقت المتبقي: {tracker.hours_remaining:.1f} ساعة"
            )
        
        await retry_purchase(session, tracker)
        
        async with retry_lock:
            if tracker.retry_key not in retry_tasks:
                break  # نجح الشراء أو تم إزالته

    # إذا انتهى الوقت ولم ينجح
    async with retry_lock:
        if tracker.retry_key in retry_tasks:
            enqueue_message(
                RETRY_MESSAGES["retry_expired"].format(
                    name=tracker.slug,
                    chain=tracker.chain_name,
                    wallet=tracker.wallet_name,
                    attempts=tracker.attempt_count,
                    hours=MAX_RETRY_HOURS
                )
            )
            retry_tasks.pop(tracker.retry_key, None)
            

# ===================================================================
# منطق التحقق والشراء الأساسي (معالجة مجموعة واحدة)
# ===================================================================
# هذه الدالة هي قلب النظام:
# 1. تجلب تفاصيل الدروب
# 2. تتحقق من أنه مينت مجاني بدأ اليوم
# 3. تجري الفحوصات السريعة على كل سلسلة لكل محفظة
# 4. تنفذ عمليات الشراء بالتوازي (كل محفظة × كل سلسلة)
# 5. عند الفشل، تجدول إعادة المحاولة (لمدة 8 ساعات)

async def evaluate_and_buy(
    session: aiohttp.ClientSession,
    slug: str,
    notified: set,
    known_external: set,
    checking: set,
):
    """
    معالجة مجموعة واحدة: فحص + شراء من جميع المحافظ على جميع السلاسل بالتوازي.
    
    هذه الدالة تتخذ القرارات التالية:
    - هل المينت مجاني؟ (إذا لا، نتجاهل)
    - هل بدأ اليوم؟ (إذا لا، نتجاهل)
    - هل هناك كمية متبقية؟ (إذا لا، نتجاهل)
    - هل الرصيد والغاز مناسبان؟ (إذا لا، نلغي لكل محفظة/سلسلة على حدة)
    - شراء من جميع المحافظ والسلاسل المؤهلة بالتوازي
    - عند الفشل، جدولة إعادة المحاولة
    
    كل محفظة تشتري بشكل مستقل عن الأخرى.
    كل محفظة تجرب على جميع السلاسل المتاحة.
    """
    try:
        # الخطوة 1: جلب تفاصيل الدروب من OpenSea API
        found, detail = await fetch_drop_detail_async(session, slug)
        if not found or not detail:
            known_external.add(slug)  # نضيف للمجموعات المعروفة (غير صالحة)
            return
        
        # الخطوة 2: التحقق من أن المينت نشط (is_minting = true)
        if not detail.get("is_minting"):
           known_external.add(slug)
           return
          
  
        # الخطوة 3: التحقق من وجود مرحلة نشطة
        stage = detail.get("active_stage")
        if not stage:
            known_external.add(slug)
            logging.info(f"⏭️ '{slug}': لا توجد مرحلة نشطة — تم تجاهله.")
            return

        # الخطوة 4: حساب الكمية المتبقية
        max_supply = int(detail.get("max_supply") or 0)       # أقصى كمية
        total_supply = int(detail.get("total_supply") or 0)   # الكمية المسكوكة
        remaining = max_supply - total_supply                  # الكمية المتبقية
        if remaining <= 0:                                     # إذا نفد، نتجاهل
            known_external.add(slug)
            return

        # الخطوة 5: جلب سعر ETH وفحص نوع المينت
        price_wei = int(stage.get("price", "0") or "0")       # السعر بـ Wei
        eth_price_usd = await get_eth_price_usd(session)       # سعر ETH الحالي

        # إرسال رسالة تفصيلية بمعلومات المينت إلى تيليجرام
        info_msg = build_mint_info_message(detail, eth_price_usd)
        enqueue_message(info_msg)

        if not is_free_or_negligible(price_wei, eth_price_usd):
            # مينت مدفوع — عرض المعلومات فقط وتجاهل الشراء حالياً
            # ملاحظة: لا نضيفه لـ known_external لكي نعيد فحصه في الجولة القادمة (ربما يصبح مجانياً)
            logging.info(f"💰 '{slug}': مينت مدفوع ({price_wei} wei) — تم عرض المعلومات فقط. سيتم إعادة الفحص لاحقاً.")
            return

        # الخطوة 6: استخراج عنوان العقد
        contract_address = detail.get("contract_address")
        if not contract_address:
            logging.warning(f"⏭️ '{slug}': لا يوجد contract_address بالبيانات.")
            known_external.add(slug)
            return

        # الخطوة 7: استخراج الحد الأقصى لكل محفظة
        max_per_wallet_raw = stage.get("max_per_wallet")
        max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None

        # نضيف slug إلى قائمة المجموعات التي تم إشعار المستخدم بها
        notified.add(slug)

        # ===============================================================
        # تنفيذ الشراء من جميع المحافظ المتاحة على جميع السلاسل بالتوازي
        # ===============================================================
        if not w3_instances:
            logging.warning("⚠️ لا توجد سلاسل نشطة للشراء.")
            return
        if not WALLETS:
            logging.warning("⚠️ لا توجد محافظ مفعلة.")
            enqueue_message(f"⚠️ لا توجد محافظ مفعلة للمجموعة {slug}")
            return

        # تجهيز قائمة المهام (كل محفظة × كل سلسلة = مهمة واحدة)
        tasks = []
        for wallet in WALLETS:
            wallet_name = wallet["name"]
            wallet_address = wallet["address"]
            wallet_private_key = wallet["private_key"]
            
            for chain_name in ENABLED_CHAINS:
                w3 = w3_instances.get(chain_name)
                if not w3:
                    continue  # تخطي إذا لم يكن هناك اتصال

                chain_config = CHAINS_CONFIG[chain_name]
                seadrop_address = chain_config["seadrop_address"]  # عنوان عقد SeaDrop

                # --- فحوصات سريعة أولية لمنع إهدار الوقت ---
                # نتحقق من الرصيد، الغاز، وعنوان الرسوم قبل بناء المعاملة
                checks = quick_checks(
                    w3, wallet_address, eth_price_usd,
                    contract_address, seadrop_address,
                )
                if not checks["pass"]:
                    reason = checks["reason"]
                    logging.info(f"⏭️ '{slug}' على {chain_name} - {wallet_name}: {reason}")
                    
                    # نرسل إشعار بالفشل لهذه المحفظة/السلسلة
                    result = {
                        "success": False,
                        "reason": reason,
                        "balance_usd": checks.get("balance_usd", 0),
                        "gas_fee_usd": checks.get("gas_fee_usd", 0),
                        "eth_price_usd": eth_price_usd,
                    }
                    enqueue_message(build_result_message(detail, result, chain_name, wallet_name))
                    
                    # --- جدولة إعادة المحاولة إذا كان السبب قابلاً لإعادة المحاولة ---
                    if reason in RETRYABLE_REASONS:
                        async with retry_lock:
                            retry_key = f"{slug}:{wallet_address}:{chain_name}"
                            # نتأكد أن المهمة غير مسجلة مسبقًا
                            if retry_key not in retry_tasks:
                                tracker = RetryTracker(
                                    slug=slug,
                                    wallet_address=wallet_address,
                                    chain_name=chain_name,
                                    detail=detail,
                                    wallet_name=wallet_name,
                                    wallet_private_key=wallet_private_key,
                                    price_wei=price_wei,
                                    max_per_wallet=max_per_wallet,
                                    remaining_supply=remaining,
                                    eth_price_usd=eth_price_usd,
                                    original_reason=reason,
                                )
                                retry_tasks[retry_key] = tracker
                                # إنشاء مهمة إعادة محاولة غير متزامنة
                                asyncio.create_task(schedule_retry(session, tracker))
                                logging.info(f"🔄 تمت جدولة إعادة المحاولة لـ '{slug}' - {wallet_name} على {chain_name} (السبب: {reason})")
                    
                    continue  # ننتقل للمحفظة/السلسلة التالية

                # --- تجهيز مهمة الشراء (في Thread منفصل لتجنب حظر الحدث) ---
                # نستخدم asyncio.to_thread لتشغيل الدالة المتزامنة في Thread منفصل
                # هذا يسمح بتنفيذ عدة عمليات شراء بالتوازي دون حظر
                tasks.append(
                    (wallet_name, chain_name, wallet_address, wallet_private_key, asyncio.to_thread(
                        attempt_purchase,
                        w3, wallet_private_key, wallet_address,
                        contract_address, seadrop_address,
                        price_wei, max_per_wallet, remaining, eth_price_usd,
                    ))
                )

        # إذا لم تكن هناك مهام (كل المحافظ فشلت في الفحوصات السريعة)
        if not tasks:
            logging.info(f"⏭️ '{slug}': لا توجد محافظ مؤهلة للشراء.")
            return

        # تسجيل عدد المحاولات
        logging.info(f"🔄 '{slug}': بدء {len(tasks)} محاولة شراء ({len(WALLETS)} محفظة × {len(ENABLED_CHAINS)} سلسلة)")

        # --- تنفيذ جميع محاولات الشراء بالتوازي ---
        # asyncio.gather تنفذ كل المهام في نفس الوقت
        results = await asyncio.gather(*[t[4] for t in tasks], return_exceptions=True)

        # معالجة النتائج لكل محفظة/سلسلة
        for (wallet_name, chain_name, wallet_address, wallet_private_key, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                # إذا كانت النتيجة خطأ غير متوقع
                logging.error(f"❌ '{slug}' على {chain_name} - {wallet_name}: خطأ غير متوقع: {result}")
                # جدولة إعادة المحاولة للخطأ غير المتوقع
                async with retry_lock:
                    retry_key = f"{slug}:{wallet_address}:{chain_name}"
                    if retry_key not in retry_tasks:
                        tracker = RetryTracker(
                            slug=slug,
                            wallet_address=wallet_address,
                            chain_name=chain_name,
                            detail=detail,
                            wallet_name=wallet_name,
                            wallet_private_key=wallet_private_key,
                            price_wei=price_wei,
                            max_per_wallet=max_per_wallet,
                            remaining_supply=remaining,
                            eth_price_usd=eth_price_usd,
                            original_reason="tx_error",
                        )
                        retry_tasks[retry_key] = tracker
                        asyncio.create_task(schedule_retry(session, tracker))
                continue
            
            if isinstance(result, dict):
                # إضافة سعر ETH للنتيجة (للرسالة)
                result["eth_price_usd"] = eth_price_usd
                
                # إرسال إشعار تيليجرام بالنتيجة
                enqueue_message(build_result_message(detail, result, chain_name, wallet_name))
                
                if result.get("success"):
                    logging.info(f"✅ '{slug}' على {chain_name} - {wallet_name}: شراء ناجح - {result['quantity']} قطعة")
                else:
                    reason = result.get("reason", "غير معروف")
                    logging.info(f"⏭️ '{slug}' على {chain_name} - {wallet_name}: {reason}")
                    
                    # --- جدولة إعادة المحاولة إذا كان السبب قابلاً لإعادة المحاولة ---
                    if reason in RETRYABLE_REASONS:
                        async with retry_lock:
                            retry_key = f"{slug}:{wallet_address}:{chain_name}"
                            if retry_key not in retry_tasks:
                                tracker = RetryTracker(
                                    slug=slug,
                                    wallet_address=wallet_address,
                                    chain_name=chain_name,
                                    detail=detail,
                                    wallet_name=wallet_name,
                                    wallet_private_key=wallet_private_key,
                                    price_wei=price_wei,
                                    max_per_wallet=max_per_wallet,
                                    remaining_supply=remaining,
                                    eth_price_usd=eth_price_usd,
                                    original_reason=reason,
                                )
                                retry_tasks[retry_key] = tracker
                                asyncio.create_task(schedule_retry(session, tracker))
                                logging.info(f"🔄 تمت جدولة إعادة المحاولة لـ '{slug}' - {wallet_name} على {chain_name} (السبب: {reason})")

    except Exception as e:
        logging.error(f"خطأ غير متوقع بمعالجة '{slug}': {e}")
    finally:
        # نزيل slug من قائمة المجموعات قيد الفحص (سواء نجحنا أو فشلنا)
        checking.discard(slug)

# ===================================================================
# الاتصال بـ OpenSea Stream عبر WebSocket
# ===================================================================
# هذه الدالة تحافظ على اتصال WebSocket مع OpenSea لاستقبال
# إشعارات فورية عند حصول مينت جديد

async def listen_opensea(notified: set, known_external: set, checking: set):
    """
    الاستماع إلى OpenSea Stream لرصد المينتات الجديدة.

    هذه الدالة:
    1. تتصل بـ WebSocket الخاص بـ OpenSea
    2. تنضم إلى قناة collection:* (كل المجموعات)
    3. تستقبل أحداث item_transferred
    4. تكتشف المينتات الجديدة (من ZERO_ADDRESS)
    5. ترسل slug للمعالجة

    تعمل في حلقة لا نهائية مع إعادة اتصال تلقائي عند انقطاع الاتصال.

    المعاملات:
        notified: set - المجموعات التي تم إشعار المستخدم بها (مشترك مع الماسح)
        known_external: set - المجموعات المعروفة/غير الصالحة (مشترك مع الماسح)
        checking: set - المجموعات قيد الفحص (مشترك مع الماسح)
    """
    msg_ref = 0                       # عداد الرسائل

    # جلسة HTTP مشتركة (تستخدمها كل الدوال)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # الاتصال بـ WebSocket
                async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                    logging.info("🚀 متصل بـ OpenSea Stream — Ethereum + Robinhood")
                    
                    # إرسال طلب الانضمام للقناة
                    join_ref = str(msg_ref)
                    await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                    msg_ref += 1
                    
                    # متغيرات تتبع الوقت
                    last_heartbeat = time.time()      # آخر نبضة حياة
                    last_stats_time = time.time()     # آخر إحصائيات

                    # حلقة استقبال الرسائل
                    while True:
                        # إحصائيات دورية كل 5 دقائق
                        if time.time() - last_stats_time > 300:
                            actives = len(checking)
                            retry_count = len(retry_tasks)
                            logging.info(f"📊 إحصائيات: {len(notified)} معروف | {len(known_external)} خارجي | {actives} قيد الفحص | {retry_count} إعادة محاولة | {len(WALLETS)} محفظة")
                            last_stats_time = time.time()

                        # إرسال نبضات حياة للحفاظ على الاتصال
                        if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                            hb_ref = str(msg_ref)
                            await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                            msg_ref += 1
                            last_heartbeat = time.time()

                        # استقبال رسالة (مع timeout)
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                        except asyncio.TimeoutError:
                            continue  # إذا انتهى الوقت دون رسالة، نستمر

                        # تحليل JSON
                        try:
                            parsed = json.loads(raw)
                        except json.JSONDecodeError:
                            continue  # رسالة غير صالحة

                        # تنسيق OpenSea: [join_ref, ref, topic, event, payload]
                        if isinstance(parsed, list) and len(parsed) == 5:
                            _jref, _ref, _topic, event_name, payload_wrapper = parsed
                        else:
                            continue

                        # نهتم فقط بأحداث item_transferred
                        if event_name != "item_transferred":
                            continue

                        # استخراج الـ payload
                        payload = (payload_wrapper or {}).get("payload") or {}
                        item = payload.get("item", {}) or {}
                        chain = (item.get("chain", {}) or {}).get("name", "")

                        # نقبل Robinhood + Ethereum معًا (نتجاهل باقي السلاسل)
                        if chain not in ("robinhood", "ethereum"):
                            continue

                        # التحقق من أن المرسل هو العقد (ZERO_ADDRESS) = مينت جديد
                        from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                        if from_address != ZERO_ADDRESS:
                            continue  # ليس مينت جديد

                        # استخراج slug المجموعة
                        slug = (payload.get("collection", {}) or {}).get("slug", "")
                        
                        # نتجاهل إذا:
                        # - لا يوجد slug
                        # - سبق وأشعرنا المستخدم بها
                        # - معروفة كغير صالحة
                        # - قيد الفحص حاليًا
                        if not slug or slug in notified or slug in known_external or slug in checking:
                            continue

                        # إضافة slug إلى قائمة الفحص
                        checking.add(slug)
                        logging.info(f"🔍 اكتشاف جديد: '{slug}' على {chain}")
                        
                        # إنشاء مهمة معالجة مع السيمافور (الحد من التزامن)
                        asyncio.create_task(
                            process_with_semaphore(session, slug, notified, known_external, checking)
                        )

            # معالجة أخطاء الاتصال وإعادة المحاولة
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                logging.warning(f"⚠️ انقطع الاتصال ({e}). إعادة الاتصال خلال 3 ثوانٍ...")
                await asyncio.sleep(3)
            except Exception as e:
                logging.error(f"خطأ غير متوقع: {e}. إعادة المحاولة خلال 5 ثوانٍ...")
                await asyncio.sleep(5)


async def process_with_semaphore(
    session: aiohttp.ClientSession,
    slug: str,
    notified: set,
    known_external: set,
    checking: set,
):
    """
    معالجة مجموعة مع السيمافور للحد من التزامن.
    
    نستخدم Semaphore للتأكد من أننا لا نعالج أكثر من
    MAX_CONCURRENT_MINTS مجموعة في نفس الوقت.
    
    المعاملات:
        session: aiohttp.ClientSession - جلسة HTTP
        slug: str - معرف المجموعة
        notified: set - مجموعات تم إشعار المستخدم بها
        known_external: set - مجموعات معروفة (غير صالحة)
        checking: set - مجموعات قيد الفحص
    """
    async with semaphore:
        await evaluate_and_buy(session, slug, notified, known_external, checking)


# ===================================================================
# الماسح الدوري للمينتات النشطة في السوق
# ===================================================================

async def scan_active_drops(notified: set, known_external: set, checking: set):
    """
    ماسح دوري يجلب كل المينتات النشطة حالياً في السوق من OpenSea.
    يعمل كل SCAN_INTERVAL ثانية (افتراضياً 5 دقائق).

    يكمل عمل listen_opensea بمعالجة المينتات الموجودة مسبقاً في السوق
    وليس فقط المينتات الجديدة التي تُكتشف عبر WebSocket.

    المعاملات:
        notified: set - المجموعات التي تم إشعار المستخدم بها (مشترك مع المستمع)
        known_external: set - المجموعات المعروفة/غير الصالحة (مشترك مع المستمع)
        checking: set - المجموعات قيد الفحص (مشترك مع المستمع)
    """
    logging.info(f"🔍 الماسح الدوري: بدأ العمل — فحص كل {SCAN_INTERVAL // 60} دقائق")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # جلب قائمة المينتات النشطة حالياً من OpenSea
                url = f"{DROPS_API_BASE}?is_minting=true&limit=50"
                headers = {"x-api-key": OPENSEA_API_KEY}

                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != 200:
                        logging.warning(f"[الماسح] OpenSea رد بـ HTTP {resp.status}")
                    else:
                        data = await resp.json()
                        # OpenSea قد يستخدم 'drops' أو 'results' كاسم للقائمة
                        drops = data.get("drops") or data.get("results", [])
                        logging.info(f"[الماسح] وجد {len(drops)} مينت نشط في السوق")

                        new_count = 0
                        for drop in drops:
                            # slug قد يكون في 'collection_slug' أو 'slug'
                            slug = drop.get("collection_slug") or drop.get("slug", "")
                            if not slug:
                                continue
                            # تجاهل المعروفة والمعالجة حالياً
                            if slug in notified or slug in known_external or slug in checking:
                                continue
                            checking.add(slug)
                            new_count += 1
                            logging.info(f"🔍 [الماسح] اكتشاف: '{slug}'")
                            asyncio.create_task(
                                process_with_semaphore(session, slug, notified, known_external, checking)
                            )

                        if new_count > 0:
                            logging.info(f"[الماسح] أرسل {new_count} مينت جديد للمعالجة")

            except Exception as e:
                logging.error(f"[الماسح] خطأ أثناء المسح: {e}")

            # انتظار حتى الجولة التالية
            await asyncio.sleep(SCAN_INTERVAL)


# ===================================================================
# التشغيل الرئيسي
# ===================================================================
# هذه الدالة تبدأ تشغيل النظام بالكامل

async def run():
    """
    تشغيل النظام: بدء الاستماع لـ OpenSea + مرسل تيليجرام.
    
    إذا كان BOT_ENABLED = false، لن يتم شراء أي شيء (وضع الأمان).
    """
    if not BOT_ENABLED:
        logging.warning("🔴 BOT_ENABLED=false — النظام متوقف عمدًا (وضع الأمان). لن يشتري أي شي.")
        enqueue_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false) — ما رح يشتري لين تفعّله.")
        await telegram_sender()  # نرسل رسالة الإيقاف فقط
        return

    if not WALLETS:
        logging.critical("🔴 لا توجد محافظ! تأكد من تعبئة PRIVATE_KEY و WALLET_ADDRESS في ملف .env")
        enqueue_message("🔴 لا توجد محافظ! تأكد من تعبئة PRIVATE_KEY و WALLET_ADDRESS في ملف .env")
        await telegram_sender()
        return

    # بناء تقرير حالة السلاسل والمحافظ
    chains_status = []
    for c in ENABLED_CHAINS:
        chains_status.append(CHAINS_CONFIG[c]["chain_name_display"])
    
    wallets_count = len(WALLETS)
    status_msg = "✅ نظام الشراء التلقائي v2 اشتغل الآن!\n"
    status_msg += f"📡 السلاسل النشطة: {', '.join(chains_status) if chains_status else 'لا توجد'}\n"
    status_msg += f"👛 المحافظ النشطة: {wallets_count}\n"
    status_msg += f"🔢 الحد الأقصى للمجموعات المتزامنة: {MAX_CONCURRENT_MINTS}\n"
    status_msg += f"🔄 نظام إعادة المحاولة: نشط — محاولات مستمرة وذكية لمدة {MAX_RETRY_HOURS} ساعات\n"
    status_msg += f"🔍 الماسح الدوري: نشط — يفحص المينتات كل {SCAN_INTERVAL} ثانية (يشمل فحص المينتات المدفوعة)"

    # إرسال رسالة بدء التشغيل عبر تيليجرام
    enqueue_message(status_msg)
    logging.info(status_msg)

    # مجموعات مشتركة بين الماسح والمستمع (لمنع معالجة نفس المينت مرتين)
    notified: set[str] = set()        # المجموعات التي تم إشعار المستخدم بها
    known_external: set[str] = set()  # المجموعات المعروفة (غير صالحة)
    checking: set[str] = set()        # المجموعات قيد الفحص حاليًا

    # تشغيل ثلاث مهام بالتوازي:
    # 1. الاستماع لـ OpenSea Stream (مينتات جديدة فورية)
    # 2. الماسح الدوري (مينتات موجودة في السوق كل 15 ثانية)
    # 3. إرسال رسائل تيليجرام
    await asyncio.gather(
        listen_opensea(notified, known_external, checking),
        scan_active_drops(notified, known_external, checking),
        telegram_sender(),
    )


def main():
    """
    نقطة الدخول الرئيسية للتطبيق.
    
    تحتوي على حلقة إعادة تشغيل تلقائية مع:
    - تأخير تصاعدي (backoff) يبدأ من 2 ثانية ويصل إلى 30 ثانية
    - معالجة Ctrl+C للإيقاف اليدوي
    """
    backoff = 2  # وقت الانتظار الأولي قبل إعادة التشغيل (ثوانٍ)
    
    while True:
        try:
            asyncio.run(run())  # تشغيل النظام غير المتزامن
        except KeyboardInterrupt:
            # إيقاف يدوي بـ Ctrl+C
            logging.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            # خطأ غير متوقع: إعادة تشغيل تلقائي
            logging.critical(f"توقف غير متوقع: {e}. إعادة التشغيل خلال {backoff} ثانية...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)  # مضاعفة وقت الانتظار (حد أقصى 30 ثانية)
            continue
        else:
            break  # خروج عادي من الحلقة


# نقطة البداية - عند تشغيل python main.py
if __name__ == "__main__":
    main()

