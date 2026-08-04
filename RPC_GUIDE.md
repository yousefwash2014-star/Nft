# 🌐 دليل RPC URLs - روابط Blockchain

## 📌 ما هو RPC URL؟

**RPC (Remote Procedure Call)** هو الرابط الذي يسمح للبوت بالاتصال بشبكة الإيثيريوم أو أي شبكة blockchain أخرى.

بدون RPC، لا يستطيع البوت:
- قراءة رصيد المحفظة
- إرسال معاملات الشراء
- التحقق من رسوم الغاز
- قراءة بيانات العقود

---

## 🏆 Infura (مجاني - الأفضل والأسهل)

### خطوة بخطوة للحصول على Ethereum RPC من Infura:

#### 1️⃣ إنشاء حساب في Infura

1. اذهب إلى **https://infura.io/**
2. اضغط على **"Sign Up"** (تسجيل) في الأعلى
3. سجل باستخدام بريدك الإلكتروني أو GitHub أو Google
4. اذهب إلى بريدك وفعّل الحساب (تأكيد البريد الإلكتروني)
5. سجل الدخول إلى Infura

#### 2️⃣ إنشاء مشروع جديد (API Key)

1. بعد تسجيل الدخول، ستظهر لك لوحة التحكم **Dashboard**
2. اضغط على **"Create New API Key"** (إنشاء مفتاح API جديد)

![Infura Dashboard](https://i.imgur.com/placeholder.png)

3. سيظهر مربع حوار، اختر:
   - **Network:** `Ethereum`
   - **Name:** اكتب اسم المشروع (مثلاً: `nft-bot` أو `my-project`)

#### 3️⃣ الحصول على رابط RPC

1. بعد إنشاء المشروع، ستظهر صفحة الإعدادات
2. ابحث عن قسم **"Endpoints"** (نقاط النهاية)
3. اختر **"Ethereum Mainnet"** (شبكة الإيثيريوم الرئيسية)
4. ستجد رابط مثل هذا:

```
https://mainnet.infura.io/v3/2a3b4c5d6e7f8g9h0i1j2k3l4m5n6o7p
```

> ⚠️ **الجزء المهم**: `2a3b4c5d6e7f8g9h0i1j2k3l4m5n6o7p` هذا هو **Project ID** الخاص بك

5. **انسخ الرابط بالكامل** (يبدأ بـ `https://mainnet.infura.io/v3/...`)

#### 4️⃣ وضعه في ملف .env

افتح ملف `.env` وأضف:

```env
ETHEREUM_RPC_URL=https://mainnet.infura.io/v3/2a3b4c5d6e7f8g9h0i1j2k3l4m5n6o7p
```

> استبدل الرابط برابطك الحقيقي!

---

## 💜 Alchemy (مجاني - بديل Infura)

### خطوة بخطوة للحصول على Ethereum RPC من Alchemy:

#### 1️⃣ إنشاء حساب في Alchemy

1. اذهب إلى **https://www.alchemy.com/**
2. اضغط على **"Get Started for Free"** (ابدأ مجانًا)
3. سجل باستخدام بريدك الإلكتروني أو Google أو GitHub

#### 2️⃣ إنشاء تطبيق جديد

1. بعد تسجيل الدخول، ستظهر لوحة التحكم
2. اضغط على **"Create App"** (إنشاء تطبيق)

3. إعدادات التطبيق:
   - **Name:** اكتب اسم (مثلاً: `nft-bot`)
   - **Description:** اكتب وصف (اختياري)
   - **Chain:** اختر **`Ethereum`**
   - **Network:** اختر **`Ethereum Mainnet`**

4. اضغط **"Create App"**

#### 3️⃣ الحصول على رابط RPC

1. في لوحة التحكم، ستجد تطبيقك الجديد
2. اضغط على اسم التطبيق لفتحه
3. ابحث عن قسم **"View Key"** أو **"API Key"**
4. ستجد:
   - **API Key:** مثل `abc123def456...`
   - **HTTPS URL:** مثل `https://eth-mainnet.g.alchemy.com/v2/abc123def456...`

5. **انسخ رابط HTTPS بالكامل**

#### 4️⃣ وضعه في ملف .env

```env
ETHEREUM_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/abc123def456...
```

---

## 🆓 مزودين RPC مجانيين آخرين (إذا Infura أو Alchemy ما اشتغلوا)

### 1. Chainstack (مجاني محدود)
- اذهب إلى https://chainstack.com/
- أنشئ حساب مجاني
- أنشئ مشروع > Ethereum Mainnet
- احصل على الرابط

### 2. QuickNode (مجاني محدود)
- اذهب إلى https://quicknode.com/
- أنشئ حساب
- خطة **"Free"** (9,000 طلب/يوم)
- أنشئ Ethereum Mainnet endpoint

### 3. Public RPC (مجاني - لكن بطيء)
إذا ما قدرت تسجل في أي موقع، استخدم واحد من هذه:

```env
# Public RPC (بطيء - ليس موصى به للبوت)
ETHEREUM_RPC_URL=https://eth.llamarpc.com
# أو
ETHEREUM_RPC_URL=https://rpc.ankr.com/eth
# أو
ETHEREUM_RPC_URL=https://ethereum.publicnode.com
```

> ⚠️ **تحذير**: الـ Public RPCs أبطأ وقد تفشل مع كثرة الاستخدام. استخدم Infura أو Alchemy للنتائج الأفضل.

---

## 🏴‍☠️ Robinhood Chain RPC

حاليًا شبكة Robinhood Chain ليست متاحة للعامة بعد. اتركها فارغة في ملف `.env`:

```env
ROBINHOOD_RPC_URL=
```

عندما تصبح متاحة، ستجد رابط RPC في:
- موقع Robinhood الرسمي
- https://chainlist.org/ (ابحث عن Robinhood)

---

## ✅ التحقق من أن RPC يعمل

بعد وضع الرابط في ملف `.env`، شغّل البوت:

```bash
python main.py
```

إذا رأيت:
```
✅ Ethereum Mainnet - Web3 متصل
```
✅ **مبروك! الـ RPC يشتغل بشكل صحيح!**

إذا رأيت:
```
❌ Ethereum Mainnet - فشل الاتصال: ...
```
❌ الرابط غير صحيح. تحقق من:
1. الرابط منسوخ بالكامل
2. لا توجد مسافات قبل أو بعد الرابط
3. الحساب مفعّل (إذا Infura جديد، تأكد من تفعيل البريد)

---

## 💡 نصائح سريعة

| المزود | السرعة | سهولة التسجيل | الحد المجاني |
|--------|--------|---------------|--------------|
| **Infura** ⭐ | سريع | سهل جدًا | 100,000 طلب/يوم |
| **Alchemy** ⭐ | سريع | سهل | 300,000,000 طلب/شهر |
| **Chainstack** | متوسط | سهل | 3,000,000 طلب/شهر |
| **Public RPC** | بطيء | لا يحتاج تسجيل | غير محدود |

**⭐ أنصحك بـ Infura - الأسهل والأسرع للتسجيل!**

---

## ❓ أسئلة شائعة عن RPC

**س: هل أحتاج RPC مختلف لكل سلسلة؟**
ج: نعم، كل سلسلة (Ethereum, Robinhood) لها رابط RPC مختلف.

**س: هل الـ RPC آمن؟ هل يسرق مفاتيح محفظتي؟**
ج: لا، RPC مجرد رابط اتصال بالشبكة. لا يمكنه سرقة مفاتيحك. لكن استخدم مزود موثوق مثل Infura أو Alchemy.

**س: ماذا لو كان الـ RPC بطيئًا؟**
ج: استخدم Infura أو Alchemy - كلاهما سريع ومجاني.

**س: هل أحتاج بطاقة ائتمان للتسجيل في Infura؟**
ج: لا، Infura مجاني ولا يطلب بطاقة ائتمان للتسجيل.

**س: ماذا لو وصلت للحد المجاني؟**
ج: انشئ حساب جديد أو استخدم Alchemy أيضًا. أو استخدم Public RPC كاحتياطي.
