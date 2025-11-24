import asyncio
import os
import sys

import aioboto3
from httpx import AsyncClient
from aioaws.s3 import S3Client as AioAwsS3Client, S3Config as AioAwsS3Config
from obstore.store import S3Store

# --------- CONFIG FROM ENV ---------
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")

# Bucket where you have at least List/Get permission.
S3_TEST_BUCKET = os.getenv("S3_TEST_BUCKET", "auto-product-build-downstream").strip()

# Default maximum number of objects to list per interface
DEFAULT_MAX_ITEMS = int(os.getenv("S3_LIST_LIMIT", "100"))


def _require_creds() -> None:
    missing = [
        name
        for name, value in [
            ("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID),
            ("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY),
        ]
        if not value
    ]
    if missing:
        print("Missing required env vars:", ", ".join(missing), file=sys.stderr)
        sys.exit(1)


# --------------------------------------------------------------------
# aioboto3: check *authentication only* via STS.GetCallerIdentity
# --------------------------------------------------------------------
async def check_aioboto3_sts() -> None:
    """
    Cleanest way to check that credentials are valid:
    STS.GetCallerIdentity doesn't require any S3 permissions.
    """
    print("\n=== aioboto3: STS GetCallerIdentity ===")
    session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    async with session.client("sts") as sts:
        resp = await sts.get_caller_identity()
        print("  Account :", resp["Account"])
        print("  ARN     :", resp["Arn"])
        print("  UserId  :", resp["UserId"])
        print("  -> aioboto3 credentials are valid")


# --------------------------------------------------------------------
# aioaws: minimal S3 call on a specific bucket
# --------------------------------------------------------------------
async def check_aioaws_s3() -> None:
    print(f"\n=== aioaws: S3 list sanity check on bucket {S3_TEST_BUCKET!r} ===")

    if not S3_TEST_BUCKET:
        print("  S3_TEST_BUCKET not set; skipping aioaws S3 check.")
        return

    async with AsyncClient(timeout=10) as http_client:
        s3 = AioAwsS3Client(
            http_client,
            AioAwsS3Config(
                AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY,
                AWS_REGION,
                S3_TEST_BUCKET,
            ),
        )

        try:
            count = 0
            async for obj in s3.list():
                # obj has `.key` for the S3 object key
                print("  First key from list():", obj.key)
                count += 1
                if count >= 1:
                    break

            if count == 0:
                print("  list() returned no objects, but request was accepted.")
            print("  -> aioaws S3 request signed & accepted")
        except Exception as e:
            print("  aioaws S3 call failed:", repr(e))


# --------------------------------------------------------------------
# obstore: S3Store and list_async() on S3_TEST_BUCKET
# --------------------------------------------------------------------
async def check_obstore_s3() -> None:
    print(f"\n=== obstore: S3Store list_async sanity check on bucket {S3_TEST_BUCKET!r} ===")

    if not S3_TEST_BUCKET:
        print("  S3_TEST_BUCKET not set; skipping obstore S3 check.")
        return

    # S3Store will read creds from env; we pass region explicitly.
    store = S3Store(
        bucket=S3_TEST_BUCKET,
        prefix="",   # list from bucket root
        region=AWS_REGION,
    )

    try:
        # list_async() yields *batches* (Sequence[ObjectMeta] or Sequence[dict])
        batch_count = 0
        async for batch in store.list_async(prefix=""):
            batch_count += 1
            if not batch:
                continue

            first = batch[0]

            # Handle both older dict-based and newer ObjectMeta-based APIs
            if isinstance(first, dict):
                # Older API: dict with "path" / "size" etc.
                name = first.get("path") or first
            else:
                # Newer API: ObjectMeta with `.path`
                name = getattr(first, "path", str(first))

            print("  First path from list_async():", name)
            print("  -> obstore S3 request signed & accepted")
            break

        if batch_count == 0:
            print("  list_async() returned no batches, but request was accepted.")
    except Exception as e:
        msg = str(e)
        if "NoSuchBucket" in msg:
            print(
                "  -> credentials are OK, but bucket does not exist or is wrong:",
                S3_TEST_BUCKET,
            )
        else:
            print("  obstore S3 call failed:", msg)


# --------------------------------------------------------------------
# Full listings with a configurable max_items
# --------------------------------------------------------------------
async def list_aioboto3_contents(max_items: int = 100) -> None:
    print(
        f"\n=== aioboto3: listing up to {max_items} objects "
        f"from bucket {S3_TEST_BUCKET!r} ==="
    )
    if not S3_TEST_BUCKET:
        print("  S3_TEST_BUCKET not set; skipping.")
        return

    session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    async with session.client("s3") as s3:
        paginator = s3.get_paginator("list_objects_v2")
        count = 0

        async for page in paginator.paginate(Bucket=S3_TEST_BUCKET):
            contents = page.get("Contents", [])
            for obj in contents:
                print("  -", obj["Key"])
                count += 1
                if count >= max_items:
                    break
            if count >= max_items:
                break

        print(f"  -> aioboto3 listed {count} objects (limit {max_items}).")


async def list_aioaws_contents(max_items: int = 100) -> None:
    print(
        f"\n=== aioaws: listing up to {max_items} objects "
        f"from bucket {S3_TEST_BUCKET!r} ==="
    )
    if not S3_TEST_BUCKET:
        print("  S3_TEST_BUCKET not set; skipping.")
        return

    async with AsyncClient(timeout=30) as http_client:
        s3 = AioAwsS3Client(
            http_client,
            AioAwsS3Config(
                AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY,
                AWS_REGION,
                S3_TEST_BUCKET,
            ),
        )

        count = 0
        try:
            async for obj in s3.list():
                print("  -", obj.key)
                count += 1
                if count >= max_items:
                    break

            print(f"  -> aioaws listed {count} objects (limit {max_items}).")
        except Exception as e:
            print("  aioaws bucket listing failed:", e)


async def list_obstore_contents(max_items: int = 100) -> None:
    print(
        f"\n=== obstore: listing up to {max_items} objects "
        f"from bucket {S3_TEST_BUCKET!r} ==="
    )
    if not S3_TEST_BUCKET:
        print("  S3_TEST_BUCKET not set; skipping.")
        return

    store = S3Store(
        bucket=S3_TEST_BUCKET,
        prefix="",
        region=AWS_REGION,
    )

    count = 0
    try:
        async for batch in store.list_async(prefix=""):
            for entry in batch:
                if isinstance(entry, dict):
                    name = entry.get("path") or entry
                else:
                    name = getattr(entry, "path", str(entry))

                print("  -", name)
                count += 1
                if count >= max_items:
                    break
            if count >= max_items:
                break

        print(f"  -> obstore listed {count} objects (limit {max_items}).")
    except Exception as e:
        print("  obstore bucket listing failed:", e)


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
async def main() -> None:
    _require_creds()

    # CLI override: python auth.py 50
    max_items = DEFAULT_MAX_ITEMS
    if len(sys.argv) > 1:
        try:
            max_items = int(sys.argv[1])
        except ValueError:
            print(
                f"Invalid max_items {sys.argv[1]!r}; "
                f"falling back to default {DEFAULT_MAX_ITEMS}",
                file=sys.stderr,
            )

    # Auth checks
    await check_aioboto3_sts()
    await check_aioaws_s3()
    await check_obstore_s3()

    # Bucket listings (limited)
    await list_aioboto3_contents(max_items=max_items)
    await list_aioaws_contents(max_items=max_items)
    await list_obstore_contents(max_items=max_items)


if __name__ == "__main__":
    asyncio.run(main())

