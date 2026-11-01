# تشغيل البوت على Railway

Railway يحقن المتغيرات في `os.environ` تلقائيًا؛ لا ترفع ملف `.env` إلى المشروع.

## الإعداد

1. أنشئ خدمة Worker في Railway واربط المستودع أو ارفع هذه الملفات.
2. في Variables أضف المتغيرات الموجودة في `.env.example`.
3. استخدم أمر التشغيل:

```text
python3 main.py
```

ملف `railway.toml` و`Procfile` يضبطان الأمر تلقائيًا.

## المتغيرات الأساسية

```text
BOT_ENABLED=true
OPENSEA_API_KEY=...
PRIVATE_KEYS=0xKEY1,0xKEY2
WALLETS=0xADDRESS1,0xADDRESS2
ETHEREUM_RPC_URL=https://rpc1,https://rpc2
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
TELEGRAM_NOTIFY_FAILURES=true
```

القيم التي تحتوي عدة عناصر تبقى مفصولة بفواصل، مثل `PRIVATE_KEYS` و`WALLETS` و`ETHEREUM_RPC_URL`.

## اختبار المتغيرات من Railway

من جهازك بعد ربط المشروع:

```bash
railway variable list --kv
railway run python3 main.py
```

لا تطبع القيم السرية في السجلات ولا ترفع `.env` إلى Git.
