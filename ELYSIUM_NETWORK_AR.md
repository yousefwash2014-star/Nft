# شبكة Elysium المدمجة

تمت إضافة Elysium Mainnet داخل الكود دون الحاجة إلى متغيرات استضافة:

- Chain ID: `1339`
- العملة الأصلية للغاز: `ELY`
- RPC الأساسي: `https://rpc.elysiumchain.tech`
- RPC احتياطي عام: `https://rpc.elysiumchain.us`
- WebSocket: `wss://ws.elysiumchain.tech`
- المستكشف: `https://explorer.elysiumchain.tech/tx/{tx}`
- عنوان SeaDrop المدمج: `0x00005EA00Ac477B1030CE78506496e8C2dE24bf5`

مصدر البيانات الرسمي: https://docs.elysiumchain.tech/docs/network-endpoints
مصدر RPC الإضافي: https://rpc.info/elysium-mainnet

لا تستخدم Atlantis/Chain ID 1338 مع أموال Mainnet؛ فهي شبكة اختبار.

## طريقة الشراء

فحص RPC الرسمي أظهر أن عنوان SeaDrop القياسي لا يحتوي عقدًا على Elysium، لذلك لا يستخدم الكود `mintPublic` العام لهذه الشبكة. مسار Elysium محصور في OpenSea Drops API؛ يرفض أي `value` غير صفري، ثم ينفذ `eth_call` و`estimate_gas` قبل البث. إذا لم يُرجع OpenSea calldata صالحة فلن تُرسل أي معاملة.
