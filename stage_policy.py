"""سياسة قبول مراحل OpenSea قبل أي إرسال on-chain."""


def stage_rejection_reason(stage: dict) -> str | None:
    """يعيد سبب الرفض، أو None إذا كانت المرحلة عامة ومسموحاً بها."""
    for field in ("eligible", "is_eligible", "wallet_eligible"):
        if field in stage and stage[field] is False:
            return f"{field}=false"
    if stage.get("eligibility") in (False, "ineligible", "not_eligible"):
        return "eligibility=ineligible"

    # OpenSea قد يرفق بيانات allowlist عامة في كائن Public Stage نفسه.
    # إذا كانت المرحلة معلّمة صراحةً Public/Eligible، فلا نرفضها بسبب
    # metadata تخص مرحلة سابقة؛ الرفض الصريح أعلاه يظل أولوية.
    eligibility = stage.get("eligibility")
    eligibility_status = ""
    if isinstance(eligibility, dict):
        eligibility_status = str(
            eligibility.get("status") or eligibility.get("state") or eligibility.get("result") or ""
        ).strip().lower()
    else:
        eligibility_status = str(eligibility or "").strip().lower()
    public_text = " ".join(
        str(stage.get(field, "")) for field in ("name", "stage_name", "label", "type", "stage_type")
    ).strip().lower()
    public_hint = (
        stage.get("is_public") is True
        or stage.get("public") is True
        or str(stage.get("type", "")).strip().lower() == "public"
        or str(stage.get("stage_type", "")).strip().lower() == "public"
        or eligibility_status in ("eligible", "public")
        or "public stage" in public_text
    )
    if public_hint:
        return None

    text = " ".join(
        str(stage.get(field, ""))
        for field in ("name", "stage_name", "label", "type", "stage_type")
    ).lower()
    restricted_words = ("allowlist", "allow list", "whitelist", "presale", "pre-sale", "private")
    matched = next((word for word in restricted_words if word in text), None)
    if matched:
        return f"restricted={matched}"

    for field in ("allowlist", "allowlist_id", "allowlist_name", "merkle_root", "proof"):
        value = stage.get(field)
        if value not in (None, "", [], {}, False):
            return f"{field}=present"
    if stage.get("is_public") is False or stage.get("public") is False:
        return "public=false"
    return None


def is_public_eligible_stage(stage: dict) -> bool:
    """يمنع مراحل Allowlist غير المؤهل لها قبل أي إرسال on-chain."""
    return stage_rejection_reason(stage) is None
