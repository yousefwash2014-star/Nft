# 🚀 NFT Intelligence Layer - نظام شراء NFT التلقائي

نظام ذكي يكتشف المينتات المجانية الجديدة على **Ethereum** و **Robinhood Chain** ويشتريها تلقائيًا!

## 📋 المتطلبات الأساسية

1. **Python 3.10+** مثبت على جهازك
2. **محفظة إيثيريوم** (OKX Wallet أو أي محفظة أخرى)
3. **حساب OpenSea** للحصول على API Key
4. **حساب تيليجرام** وبوت للإشعارات
5. **رابط RPC** واحد على الأقل (من Infura أو Alchemy)

---

## 🔧 خطوات التشغيل الكاملة

### الخطوة 1: تثبيت Python
- تأكد من تثبيت Python 3.10 أو أحدث
- افتح Terminal/CMD واكتب: `python --version`
- إذا لم يكن مثبتًا، حمّله من: https://www.python.org/downloads/

### الخطوة 2: تثبيت المكتبات
افتح Terminal في مجلد المشروع واكتب:
```bash
pip install -r requirements.txt
```

### الخطوة 3: الحصول على المفاتيح المطلوبة

#### 🔑 OpenSea API Key
1. اذهب إلى https://opensea.io/
2. سجل الدخول أو أنشئ حسابًا
3. اذهب إلى https://opensea.io/account
4. ابحث عن قسم "Developer" أو "API Keys"
5. اضغط "Create API Key" واتبع التعليمات
6. انسخ المفتاح (يبدأ عادةً بـ `opensea_...`)

#### 🤖 Telegram Bot Token
1. افتح تطبيق تيليجرام
2. ابحث عن `@BotFather`
3. أرسل `/newbot`
4. اختر اسمًا للبوت (مثلاً: `NFT_Buyer_Bot`)
5. اختر username (ينتهي بـ `_bot`، مثلاً: `NFT_Buyer_Bot`)
6. BotFather سيعطيك توكن مثل: `123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11`

#### 💬 Telegram Chat ID
1. ابدأ محادثة مع بوتك الجديد
2. أرسل أي رسالة (مثلاً: "Hello")
3. افتح الرابط التالي في المتصفح (استبدل TOKEN بالتوكن):
   `https://api.telegram.org/bot<TOKEN>/getUpdates`
4. ابحث عن `"chat":{"id":` في النتيجة
5. الرقم الذي يظهر هو Chat ID (مثلاً: `-123456789` أو `123456789`)

#### 👛 المفتاح الخاص للمحفظة (Private Key) من OKX Wallet
**⚠️ تحذير: هذا المفتاح يتحكم بأموالك! لا تشاركه مع أي أحد!**

**على متصفح Chrome (إضافة OKX Wallet):**
1. افتح متصفح Chrome وثبّت إضافة OKX Wallet من Chrome Web Store (إذا لم تكن مثبتة)
2. أنشئ محفظة جديدة أو استورد محفظتك الحالية
3. اضغط على أيقونة OKX Wallet في شريط الإضافات
4. اختر المحفظة التي تريد استخدامها
5. اضغط على أيقونة النقاط الثلاث (...) أو "الإعدادات" (Settings)
6. اختر **"Security & Privacy"** (الأمان والخصوصية)
7. اختر **"Export Private Key"** (تصدير المفتاح الخاص)
8. أدخل كلمة سر المحفظة (password)
9. انسخ المفتاح الخاص (يبدأ بـ `0x`...)
10. **ملاحظة**: لا تشارك هذا المفتاح مع أي أحد! يحذف مسؤوليته.

**على تطبيق OKX للجوال (iOS/Android):**
1. افتح تطبيق OKX
2. سجل الدخول إلى محفظتك
3. اضغط على أيقونة المحفظة في الأسفل
4. اختر المحفظة التي تريدها
5. اضغط على النقاط الثلاث (...) في الأعلى
6. اختر **"Wallet Details"** (تفاصيل المحفظة)
7. اختر **"Export Private Key"** (تصدير المفتاح الخاص)
8. أدخل كلمة سر المحفظة أو استخدم بصمة الإصبع
9. انسخ المفتاح (يبدأ بـ `0x`)

**كيف تحصل على عنوان المحفظة (WALLET_ADDRESS):**
1. افتح OKX Wallet
2. العنوان يظهر في أعلى الصفحة الرئيسية
3. يبدأ بـ `0x` (مثل: `0x1234...`)
4. انسخه بالكامل

#### 🌐 RPC URLs
**من Infura (مجاني - موصى به لـ Ethereum):**
1. اذهب إلى https://infura.io/
2. اشترك (مجاني)
3. أنشئ مشروع جديد (مثلاً: "NFT-Bot")
4. اختر "Ethereum" كشبكة
5. انسخ الرابط من قسم "Endpoints" > "Mainnet"
6. سيكون شكله: `https://mainnet.infura.io/v3/abc123def456...`

**من Alchemy (مجاني - بديل):**
1. اذهب إلى https://www.alchemy.com/
2. اشترك (مجاني)
3. أنشئ تطبيق جديد
4. اختر "Ethereum Mainnet"
5. انسخ الرابط من "HTTPS"

### الخطوة 4: إعداد ملف .env

1. انسخ ملف `.env.example` إلى `.env`:
   ```bash
   cp .env.example .env
   ```
   (على ويندوز: انسخ الملف وأعد تسميته إلى `.env`)

2. افتح ملف `.env` في محرر نصوص

3. املأ القيم التالية (كلها إجبارية ما لم يُذكر):

```env
# مثال لملف .env مكتمل:
OPENSEA_API_KEY=opensea_abc123def456...
TELEGRAM_BOT_TOKEN=123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11
TELEGRAM_CHAT_ID=-123456789
PRIVATE_KEY=0xabc123def456... (مفتاحك الخاص)
WALLET_ADDRESS=0x1234... (عنوان محفظتك)
ETHEREUM_RPC_URL=https://mainnet.infura.io/v3/abc123def456...
ROBINHOOD_RPC_URL=          # اتركه فارغًا إذا لا تريد Robinhood
BOT_ENABLED=true
```

### الخطوة 5: تشغيل البوت

```bash
python main.py
```

إذا اشتغل بشكل صحيح، سترى:
```
🚀 متصل بـ OpenSea Stream — Ethereum + Robinhood
✅ نظام الشراء التلقائي v2 اشتغل الآن!
```

وستصلك رسالة تأكيد على تيليجرام.

---

## 💡 نصائح مهمة

### 📊 إضافة محافظ متعددة في نفس التشغيلة
النظام يدعم **حتى 5 محافظ في نفس الوقت**! كل ما عليك هو إضافتها في ملف `.env`:

```env
# مثال: 3 محافظ
PRIVATE_KEY=0xabc...     # المحفظة 1
WALLET_ADDRESS=0x123...

WALLET_2_PRIVATE_KEY=0xdef...    # المحفظة 2
WALLET_2_ADDRESS=0x456...

WALLET_3_PRIVATE_KEY=0xghi...    # المحفظة 3
WALLET_3_ADDRESS=0x789...
```

البوت سيشتري من **جميع المحافظ النشطة** في نفس الوقت لكل مينت. لا حاجة لتشغيل نسخ متعددة!

#### 🔑 كيفية إنشاء محافظ متعددة في OKX Wallet (للمحفظة 2، 3، 4، 5):

**الطريقة 1: إنشاء محفظة جديدة في إضافة Chrome**
1. افتح إضافة OKX Wallet في Chrome
2. اضغط على أيقونة المحفظة في الأعلى (تظهر اسم المحفظة الحالية)
3. اختر **"Create Wallet"** (إنشاء محفظة) أو **"Add Wallet"** (إضافة محفظة)
4. اختر **"Create a new wallet"** (إنشاء محفظة جديدة)
5. احفظ الـ **Seed Phrase** (عبارة الاسترداد) في مكان آمن جدًا! **هذه هي الطريقة الوحيدة لاستعادة المحفظة إذا فقدت الوصول**
6. أدخل كلمة سر جديدة للمحفظة (أو استخدم نفسها)
7. الآن لديك محفظة جديدة
8. كرر العملية لإنشاء محفظة 3 و 4 و 5

**الطريقة 2: إنشاء محفظة جديدة من التطبيق على سطح المكتب**
1. افتح OKX Wallet Extension
2. اضغط على اسم المحفظة في الأعلى
3. اختر "Add Wallet" > "Create a new wallet"
4. احفظ Seed Phrase
5. أنشئ كلمة سر

#### 🔑 الحصول على المفتاح الخاص (Private Key) لكل محفظة:

بعد إنشاء المحافظ، اتبع هذه الخطوات لكل محفظة على حدة:

1. اضغط على أيقونة OKX Wallet في Chrome
2. اختر المحفظة التي تريد (من القائمة المنسدلة في الأعلى)
3. اضغط على النقاط الثلاث (...) > اختر **"Security & Privacy"**
4. اختر **"Export Private Key"**
5. أدخل كلمة سر المحفظة
6. انسخ المفتاح الخاص (يبدأ بـ `0x...`)
7. عنوان المحفظة يظهر في أعلى الصفحة الرئيسية

**كرر هذه الخطوات لكل محفظة (2، 3، 4، 5) وضعهم في ملف `.env`:**

```env
# المحفظة 1
PRIVATE_KEY=0xabc456...     # من أول محفظة
WALLET_ADDRESS=0x123456...

# المحفظة 2
WALLET_2_PRIVATE_KEY=0xdef789...    # من ثاني محفظة
WALLET_2_ADDRESS=0x456789...

# المحفظة 3
WALLET_3_PRIVATE_KEY=0xghi012...    # من ثالث محفظة
WALLET_3_ADDRESS=0x789012...

# المحفظة 4
WALLET_4_PRIVATE_KEY=0xjkl345...    # من رابع محفظة
WALLET_4_ADDRESS=0x012345...

# المحفظة 5
WALLET_5_PRIVATE_KEY=0xmno678...    # من خامس محفظة
WALLET_5_ADDRESS=0x345678...
```

**ملاحظة مهمة**: 
- كل محفظة تحتاج رصيد ETH منفصل لتغطية رسوم الغاز
- احفظ Seed Phrase لكل محفظة في مكان آمن ومنفصل
- البوت سيحاول الشراء من جميع المحافظ النشطة التي لديها رصيد كافٍ

### 🔒 الأمان
- **لا تشارك ملف `.env` مع أي أحد!** يحتوي على مفاتيحك الخاصة
- أضف `.env` إلى `.gitignore` قبل رفع المشروع إلى GitHub
- استخدم محفظة برصيد صغير للتجربة أولاً
- البوت يشتري فقط المينتات المجانية (أقل من $0.01)

### ⚡ تحسينات الأداء
- `MAX_CONCURRENT_MINTS = 3` : أقصى عدد مجموعات يتعامل معها البوت في نفس الوقت
- `ETH_PRICE_CACHE_TTL = 60` : تحديث سعر ETH كل 60 ثانية
- كل هذه الإعدادات في بداية ملف `main.py`

### 🛑 إيقاف البوت
- اضغط `Ctrl + C` في Terminal لإيقاف البوت يدويًا
- أو عدّل `BOT_ENABLED=false` في ملف `.env` وأعد التشغيل

---

## ❓ أسئلة شائعة

**س: هل البوت يشتري NFT برسوم غاز عالية؟**
ج: لا، البوت ملغي الشراء إذا رسوم الغاز تجاوزت $0.05 (قابل للتعديل).

**س: هل أحتاج ETH في المحفظة للمينت المجاني؟**
ج: نعم، تحتاج ETH لتغطية رسوم الغاز فقط (حتى لو المينت مجاني).

**س: ماذا لو كان RPC الخاص بي بطيئًا؟**
ج: استخدم Infura أو Alchemy - كلاهما مجاني وسريع.

**س: كم أحتاج من ETH في المحفظة؟**
ج: الحد الأدنى $0.30 (30 سنت) لرسوم الغاز. يُفضل أن يكون لديك $5-$10 للراحة.

**س: هل يدعم النظام سلاسل أخرى؟**
ج: حاليًا يدعم Ethereum و Robinhood Chain فقط. يمكن إضافة سلاسل أخرى بتعديل الكود.

---

## 📁 هيكل المشروع

```
nft-intelligence-layer-main/
├── main.py              # النظام الرئيسي (الاتصال بـ OpenSea + إدارة العمليات)
├── buyer.py             # محرك الشراء (التحقق من الرصيد + الغاز + تنفيذ المعاملة)
├── requirements.txt     # قائمة المكتبات المطلوبة
├── .env                 # ملف الإعدادات (مفاتيح API + المحفظة) - أنشئه بنفسك
├── .env.example         # نموذج لملف الإعدادات
├── .gitignore           # يمنع رفع .env إلى GitHub
├── README.md            # هذا الملف - دليل الاستخدام
└── TODO.md              # سجل المهام المنجزة
```

---

## 📞 الدعم

إذا واجهتك أي مشكلة:
1. تأكد من تثبيت جميع المكتبات في `requirements.txt`
2. تأكد من صحة المفاتيح في ملف `.env`
3. تأكد من وجود رصيد كافٍ في المحفظة
4. أعد تشغيل البوت

**البوت الآن جاهز للتشغيل! 🚀**

---

## ☁️ تشغيل البوت 24/7 على الإنترنت (استضافة)

البوت يحتاج إلى جهاز يعمل 24 ساعة حتى يبقى متصلاً بـ OpenSea. إليك أفضل الخيارات المجانية لاستضافته على الإنترنت:

### 🆓 الخيار 1: Render (مجاني - الأسهل)

**خطوات النشر:**
1. ارفع الكود إلى GitHub:
   ```bash
   git init
   git add .
   git commit -m "initial commit"
   # أنشئ Repository على GitHub وارفع الكود
   ```

2. اذهب إلى **https://dashboard.render.com** > New+ > Web Service

3. اربط GitHub واختار المشروع

4. إعدادات الخدمة:
   - **Name:** `nft-bot`
   - **Runtime:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python main.py`
   - **Plan:** **Free**

5. أضف متغيرات البيئة (Environment Variables) من ملف `.env`:
   - `OPENSEA_API_KEY=key...`
   - `TELEGRAM_BOT_TOKEN=token...`
   - `TELEGRAM_CHAT_ID=id...`
   - `PRIVATE_KEY=0x...`
   - `WALLET_ADDRESS=0x...`
   - `ETHEREUM_RPC_URL=https://...`
   - `BOT_ENABLED=true`

6. اضغط Create Web Service ✅

### 🆓 الخيار 2: Railway (مجاني مع $5 هدية)

1. اذهب إلى **https://railway.app**
2. New Project > Deploy from GitHub
3. اختر المشروع
4. أضف متغيرات البيئة (نفس الخطوات)
5. Start Command: `python main.py`
6. ✅

### 🆓 الخيار 3: GitHub Actions (مجاني لكن محدود)

أنشئ ملف `.github/workflows/bot.yml`:

```yaml
name: NFT Bot
on:
  schedule:
    - cron: '*/5 * * * *'
  workflow_dispatch:

jobs:
  run-bot:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -r requirements.txt
      - run: python main.py
        env:
          OPENSEA_API_KEY: ${{ secrets.OPENSEA_API_KEY }}
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          PRIVATE_KEY: ${{ secrets.PRIVATE_KEY }}
          WALLET_ADDRESS: ${{ secrets.WALLET_ADDRESS }}
          ETHEREUM_RPC_URL: ${{ secrets.ETHEREUM_RPC_URL }}
          WALLET_2_PRIVATE_KEY: ${{ secrets.WALLET_2_PRIVATE_KEY }}
          WALLET_2_ADDRESS: ${{ secrets.WALLET_2_ADDRESS }}
          BOT_ENABLED: true
```

ثم أضف المتغيرات في Settings > Secrets and variables > Actions

### 💰 الخيار 4: VPS سيرفر خاص (من $5/شهر)

**مزودين رخيصين:**
- **Hetzner** (€4/شهر - https://hetzner.com)
- **DigitalOcean** ($6/شهر)
- **Vultr** ($6/شهر)

**خطوات التشغيل:**
```bash
ssh root@your-server-ip
apt update && apt install python3 python3-pip screen git -y
git clone https://github.com/your-username/your-repo.git
cd your-repo
pip install -r requirements.txt
# أنشئ ملف .env واملأه
nano .env
# شغّل البوت في الخلفية
screen -S nftbot
python main.py
# افصل بـ Ctrl+A ثم D
```

### 💡 مقارنة الخيارات:

| الخيار | السعر | وقت التشغيل | سهولة |
|--------|-------|-------------|-------|
| **Render** | مجاني | 24/7 (ينام 15 د) | سهل جدًا ⭐ |
| **Railway** | مجاني + $5 | 24/7 | سهل |
| **GitHub Actions** | مجاني | 2000 د/شهر | متوسط |
| **VPS** | $5/شهر | 24/7 كامل | صعب |


