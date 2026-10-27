from conftest import read_first_payload, wait_for_payload, wait_until


async def wait_for_query_payload(session, key_expr: str, payload: bytes) -> bytes:
    async def reply_is_available() -> bytes | None:
        replies = [reply for reply in session.get(key_expr, timeout=1.0) if reply.ok]
        for reply in replies:
            actual = reply.ok.payload.to_bytes()
            if actual == payload:
                return actual
        return None

    return await wait_until(
        reply_is_available,
        f"query reply for '{key_expr}'",
    )
